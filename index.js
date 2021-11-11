const express = require('express')
const duckdb = require('duckdb');
const app = express()
const port = 3000

app.get('/', (req, res) => {
    sql_pivot(function(result) {
        console.log('result:',result);
        res.send('My result is:\n'+result);
    });
})

function sql_pivot(callback_fn) {
    
    var db = new duckdb.Database(':memory:'); // or a file name for a persistent DB
    db.all('SELECT 42 AS fortytwo', function(err, res) {
        if (err) {
            throw err;
        }
        console.log(res[0].fortytwo)
        callback_fn(res[0].fortytwo);
    });
}

app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
})