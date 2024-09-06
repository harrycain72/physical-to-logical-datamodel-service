const express = require('express');
const path = require('path');

const app = express();
const port = process.env.PORT || 4000;  // Use environment variable or default to 3000

app.use(express.static('public'));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
}).on('error', (e) => {
  if (e.code === 'EADDRINUSE') {
    console.log('Port is busy, trying the next one...');
    app.listen(0); // This will find an available port
  } else {
    console.error('An error occurred:', e);
  }
});