const BpmnModdle = require('bpmn-moddle');
const fs = require('fs');

async function createPizzaOrderBPMN() {
  const moddle = new BpmnModdle();

  // Create the root element
  const definitions = moddle.create('bpmn:Definitions', {
    targetNamespace: 'http://bpmn.io/schema/bpmn',
    rootElements: [] // Initialize rootElements as an empty array
  });

  // Create the process
  const process = moddle.create('bpmn:Process', { id: 'PizzaOrderProcess' });
  definitions.rootElements.push(process);

  // Create collaboration for pools
  const collaboration = moddle.create('bpmn:Collaboration', { id: 'Collaboration_1' });
  definitions.rootElements.push(collaboration);

  // Create pools and lanes
  const pizzaCustomerPool = moddle.create('bpmn:Participant', { 
    id: 'PizzaCustomer', 
    name: 'Pizza Customer', 
    processRef: process.id 
  });
  collaboration.participants = [pizzaCustomerPool]; // Initialize participants as an array

  const laneSet = moddle.create('bpmn:LaneSet');
  const clerkLane = moddle.create('bpmn:Lane', { id: 'Clerk', name: 'Clerk' });
  const pizzaChefLane = moddle.create('bpmn:Lane', { id: 'PizzaChef', name: 'Pizza Chef' });
  const deliveryBoyLane = moddle.create('bpmn:Lane', { id: 'DeliveryBoy', name: 'Delivery Boy' });
  laneSet.lanes = [clerkLane, pizzaChefLane, deliveryBoyLane]; // Initialize lanes as an array
  process.laneSets = [laneSet]; // Initialize laneSets as an array

  // Create start event
  const startEvent = moddle.create('bpmn:StartEvent', { id: 'StartEvent_1', name: 'Hungry for Pizza' });
  process.flowElements = [startEvent]; // Initialize flowElements as an array

  // Create tasks (add more as needed)
  const selectPizzaTask = moddle.create('bpmn:Task', { id: 'Task_SelectPizza', name: 'Select a Pizza' });
  const orderPizzaTask = moddle.create('bpmn:Task', { id: 'Task_OrderPizza', name: 'Order Pizza' });
  process.flowElements.push(selectPizzaTask, orderPizzaTask);

  // Create end event
  const endEvent = moddle.create('bpmn:EndEvent', { id: 'EndEvent_1', name: 'Hunger satisfied' });
  process.flowElements.push(endEvent);

  // Create sequence flows
  const flow1 = moddle.create('bpmn:SequenceFlow', {
    id: 'Flow_1',
    sourceRef: startEvent,
    targetRef: selectPizzaTask
  });
  const flow2 = moddle.create('bpmn:SequenceFlow', {
    id: 'Flow_2',
    sourceRef: selectPizzaTask,
    targetRef: orderPizzaTask
  });
  // Add more flows as needed
  process.flowElements.push(flow1, flow2);

  // Generate XML
  const { xml } = await moddle.toXML(definitions);

  // Save to file
  fs.writeFileSync('pizza_order_process.bpmn', xml);

  console.log('BPMN file created successfully!');
}

// Call the function
createPizzaOrderBPMN().catch(console.error);