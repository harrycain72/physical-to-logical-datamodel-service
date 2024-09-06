import os
from langchain.chat_models import init_chat_model
from langchain_core.prompts import PromptTemplate
from sqlalchemy import create_engine, MetaData, inspect
from loguru import logger
import asyncio
from functools import lru_cache
from langchain_core.output_parsers import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough
import requests
import zlib
import base64



# Configure Loguru
logger.add("app.log", rotation="500 MB", level="INFO")

# Cache the table info to avoid repeated database queries
@lru_cache(maxsize=None)
def get_table_info(db_url):
    engine = create_engine(db_url)
    metadata = MetaData()
    metadata.reflect(engine)
    inspector = inspect(engine)

    table_info = {}
    for table_name in inspector.get_table_names():
        columns = []
        for column in inspector.get_columns(table_name):
            columns.append(f"{column['name']} ({column['type']})")
        foreign_keys = inspector.get_foreign_keys(table_name)

        table_info[table_name] = {
            'columns': columns,
            'foreign_keys': foreign_keys
        }

    return table_info


def getPromptForRole(prompt):

    business_analyst = """
        As a business analyst, create a logical data model based on the given physical database structure. Focus on describing entities from a business perspective, emphasizing their role and importance in the organization's operations.

        Physical Database Structure:
        {table_info}

        Instructions:
        For each business entity identified in the physical model:

        1. Entity Name: Provide a clear, business-friendly name for the entity.

        2. Business Description: Write a brief paragraph (2-3 sentences) describing:
           - What this entity represents in the business context
           - Why it's important to the organization
           - How it relates to key business processes or operations

        3. Key Attributes: List 3-5 most important attributes that define this entity from a business perspective. For each attribute:
           - Provide a business-friendly name
           - Explain its significance (1 sentence)

        4. Business Relationships: Describe how this entity relates to other key entities in the model. Use business terminology like:
           - "is associated with"
           - "depends on"
           - "is composed of"
           - "is responsible for"

        5. Business Rules: Mention any critical business rules or policies that govern this entity (if apparent from the data structure).

        6. Data Stewardship: If relevant, suggest which department or role might be responsible for maintaining the accuracy and completeness of this entity's data.

        Present each entity description in a structured, easy-to-read format. Avoid technical database terminology; instead, use language that would be clear to non-technical stakeholders.

        Example format:

        Entity: Customer

        Business Description:
        A Customer represents an individual or organization that purchases our products or services. This entity is crucial for managing client relationships, tracking sales history, and personalizing marketing efforts. It forms the foundation of our customer-centric business model.

        Key Attributes:
        1. Customer Identifier: A unique way to recognize each customer, essential for tracking individual relationships.
        2. Contact Information: Includes methods to reach the customer, critical for communication and service delivery.
        3. Customer Segment: Categorizes customers for targeted marketing and service strategies.

        Business Relationships:
        - Is associated with multiple Orders
        - May belong to one or more Customer Groups
        - Is responsible for generating Sales Leads

        Business Rules:
        - Each customer must have at least one form of contact information.
        - Customer segmentation should be reviewed annually.

        Data Stewardship:
        The Customer Relations department is likely responsible for maintaining and updating customer data.

        Begin your business-oriented logical model below:
        """

    data_modeler = """
        You are a data modeling expert tasked with creating a logical data model (entity model) based on the given physical data model of a database.

        Physical Data Model:
        {table_info}

        Instructions:
        1. Transform the physical model into a logical entity model.
        2. Focus on business entities rather than database-specific implementations.
        3. For each entity:
           a. Identify the entity name (use business terminology)
           b. List its attributes (focus on conceptual attributes, not physical columns)
           c. Specify its primary identifier (conceptual, not necessarily the physical primary key)
           d. Describe its relationships with other entities (use business terms like "has many", "belongs to")
        4. Abstract away database-specific details such as:
           - Data types (use conceptual types instead)
           - Indexes
           - Constraints (except for essential business rules)
           - Table prefixes or suffixes
        5. Identify and name relationships between entities.
        6. If applicable, note any derived attributes or calculated fields.
        7. Include any important business rules or constraints that affect the entity relationships.

        Present the logical model in a clear, structured format, suitable for business stakeholders.
        Use natural language and avoid database-specific terminology.

        Example format for each entity:
        Entity: Customer
        Attributes:
          - Customer ID (identifier)
          - Name
          - Contact Information
        Relationships:
          - Has many Orders
          - Belongs to one Customer Category

        Begin your logical model below:
        """

    uml_modeler = """
As a UML expert, create a class diagram based on the given physical database structure.
Focus on representing the entities as classes, their attributes, and relationships.

Physical Database Structure:
{table_info}

Instructions:
1. Identify the main entities and represent them as classes.
2. For each class:
   a. List its attributes with their data types
   b. Identify the primary key attribute(s) and mark them with a stereotype <<PK>>
   c. Identify any foreign key attributes and mark them with a stereotype <<FK>>
3. Define relationships between classes:
   - Use appropriate relationship types (association, aggregation, composition)
   - Include cardinality (e.g., 1, 0..1, *, 1..*)
4. If applicable, identify and include any inheritance relationships.

Present the class diagram in PlantUML format. Use the following template and fill in the classes and relationships:

@startuml
skinparam classAttributeIconSize 0

' Define classes here
' Example:
' class Customer {{
'   <<PK>> id : int
'   name : string
'   email : string
' }}

' Define relationships here
' Example:
' Customer "1" -- "*" Order : places

@enduml

Ensure that the PlantUML code is complete and can be directly used to generate a diagram.
"""

    if prompt == "business_analyst":
        return business_analyst
    elif prompt == "data_modeler":
        return data_modeler
    elif prompt == "uml_modeler":
        return uml_modeler



async def generate_logical_model(role, db_url, model):
    try:
        logger.info(f"Generating UML class diagram for database: {db_url}")

        prompt = PromptTemplate.from_template(getPromptForRole(role))

        chain = (
                {"table_info": RunnablePassthrough()}
                | prompt
                | model
                | StrOutputParser()
        )

        # Get table info asynchronously
        logger.debug("Fetching table info")
        table_info = await asyncio.to_thread(get_table_info, db_url)

        # Run the chain asynchronously
        logger.debug("Running LLM chain")
        result = await chain.ainvoke(str(table_info))

        logger.success("Logical model generated successfully")
        return result

    except Exception as e:
        logger.error(f"Error generating logical model: {str(e)}")
        raise

def plantuml_encode(text):
    """Encodes PlantUML text to the format required by the PlantUML server."""
    compressed = zlib.compress(text.encode('utf-8'))
    encoded = base64.urlsafe_b64encode(compressed[2:-4]).decode('utf-8')
    return encoded


def visualize_uml(plantuml_code, output_file="class_diagram.png"):
    try:
        logger.info("Visualizing UML diagram")
        puml_url = 'http://localhost:8080/png/'
        encoded_diagram = plantuml_encode(plantuml_code)
        full_url = f"{puml_url}{encoded_diagram}"

        # Print the URL
        print(full_url)

        response = requests.get(full_url)

        if response.status_code == 200 and 'image/png' in response.headers.get('Content-Type', ''):
            with open(output_file, 'wb') as file:
                file.write(response.content)
            logger.success(f"UML diagram saved as {output_file}")
        else:
            logger.error(
                f"Failed to generate UML diagram: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")
            response.raise_for_status()
    except Exception as e:
        logger.exception(f"Error visualizing UML diagram: {str(e)}")
        raise



async def main():
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

    gpt = init_chat_model("gpt-3.5-turbo", model_provider="openai", temperature=0)
    anthropic= init_chat_model("claude-3-5-sonnet-20240620", model_provider="anthropic", temperature=0)
    llm = gpt

    db_url = "sqlite:///northwind.db"

    try:
        role = "uml_modeler"
        result = await generate_logical_model(role, db_url,llm)
        business_analysis = await generate_logical_model("business_analyst", db_url, llm)
        print(business_analysis)
        visualize_uml(result)

        print(result)
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Usage
if __name__ == "__main__":
    asyncio.run(main())



