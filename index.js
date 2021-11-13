const express = require('express')
const duckdb = require('duckdb');
const app = express()
const port = 3000
const cors = require('cors');

app.use(cors({
    origin: '*'
}));

app.get('/column_list', async (req, res) => {
    var db = new duckdb.Database(':memory:');
    table_create_message = await create_example_table(db, 'my_table');
    // console.log(table_create_message);
    try {
        message = await column_list(db, 'my_table')
    } catch(e) {
        message = e.message;
    }
    res.send(message);

})
app.get('/pivot', async (req, res) => {
    var db = new duckdb.Database(':memory:');
    table_create_message = await create_example_table(db, 'my_table');
    console.log(table_create_message);
    try {
        // message = await build_pivot_sql(db, req.query.table_name, req.query.rows, req.query.columns, req.query.values, req.query.filters)
        message = await example_sql(db);
    } catch(e) {
        message = e.message;
    }
    res.send(message);

})

async function create_example_table(db=undefined, table_name='my_table') {
    var sql = `CREATE TABLE my_table AS SELECT  'A' AS category,  'X' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  10 AS revenue,  99 AS inventory UNION ALL SELECT  'A' AS category,  'Y' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  20 AS revenue,  199 AS inventory UNION ALL SELECT  'A' AS category,  'Z' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  30 AS revenue,  299 AS inventory UNION ALL SELECT  'B' AS category,  'X' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  40 AS revenue,  399 AS inventory UNION ALL SELECT  'B' AS category,  'Y' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  50 AS revenue,  499 AS inventory UNION ALL SELECT  'B' AS category,  'Z' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  60 AS revenue,  599 AS inventory UNION ALL SELECT  'C' AS category,  'X' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  70 AS revenue,  699 AS inventory UNION ALL SELECT  'C' AS category,  'Y' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  80 AS revenue,  799 AS inventory UNION ALL SELECT  'C' AS category,  'Z' AS subcategory,  'Flintstones' AS product_family,  'Rock 1' AS product,  90 AS revenue,  899 AS inventory UNION ALL SELECT  'A' AS category,  'X' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  100 AS revenue,  999 AS inventory UNION ALL SELECT  'A' AS category,  'Y' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  110 AS revenue,  1099 AS inventory UNION ALL SELECT  'A' AS category,  'Z' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  120 AS revenue,  1199 AS inventory UNION ALL SELECT  'B' AS category,  'X' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  130 AS revenue,  1299 AS inventory UNION ALL SELECT  'B' AS category,  'Y' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  140 AS revenue,  1399 AS inventory UNION ALL SELECT  'B' AS category,  'Z' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  150 AS revenue,  1499 AS inventory UNION ALL SELECT  'C' AS category,  'X' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  160 AS revenue,  1599 AS inventory UNION ALL SELECT  'C' AS category,  'Y' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  170 AS revenue,  1699 AS inventory UNION ALL SELECT  'C' AS category,  'Z' AS subcategory,  'Flintstones' AS product_family,  'Rock 2' AS product,  180 AS revenue,  1799 AS inventory UNION ALL SELECT  'A' AS category,  'X' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  10000 AS revenue,  990 AS inventory UNION ALL SELECT  'A' AS category,  'Y' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  20000 AS revenue,  1990 AS inventory UNION ALL SELECT  'A' AS category,  'Z' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  30000 AS revenue,  2990 AS inventory UNION ALL SELECT  'B' AS category,  'X' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  40000 AS revenue,  3990 AS inventory UNION ALL SELECT  'B' AS category,  'Y' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  50000 AS revenue,  4990 AS inventory UNION ALL SELECT  'B' AS category,  'Z' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  60000 AS revenue,  5990 AS inventory UNION ALL SELECT  'C' AS category,  'X' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  70000 AS revenue,  6990 AS inventory UNION ALL SELECT  'C' AS category,  'Y' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  80000 AS revenue,  7990 AS inventory UNION ALL SELECT  'C' AS category,  'Z' AS subcategory,  'Jetsons' AS product_family,  'Laser 1' AS product,  90000 AS revenue,  8990 AS inventory UNION ALL SELECT  'A' AS category,  'X' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  100000 AS revenue,  9990 AS inventory UNION ALL SELECT  'A' AS category,  'Y' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  110000 AS revenue,  10990 AS inventory UNION ALL SELECT  'A' AS category,  'Z' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  120000 AS revenue,  11990 AS inventory UNION ALL SELECT  'B' AS category,  'X' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  130000 AS revenue,  12990 AS inventory UNION ALL SELECT  'B' AS category,  'Y' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  140000 AS revenue,  13990 AS inventory UNION ALL SELECT  'B' AS category,  'Z' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  150000 AS revenue,  14990 AS inventory UNION ALL SELECT  'C' AS category,  'X' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  160000 AS revenue,  15990 AS inventory UNION ALL SELECT  'C' AS category,  'Y' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  170000 AS revenue,  16990 AS inventory UNION ALL SELECT  'C' AS category,  'Z' AS subcategory,  'Jetsons' AS product_family,  'Laser 2' AS product,  180000 AS revenue,  17990 AS inventory `
    return run_query(db,sql);
}

async function column_list(db, table_name) {
    var sql = `
    select 
        a.attname as column_name
    from pg_attribute a
    join pg_class c
    on a.attrelid = c.oid
    where
        c.relname = ?::string`

    return run_query(db,sql,[table_name]);
}

async function example_sql(db) {
    var sql = `
    SELECT
          category
        , subcategory
        , MAX(revenue) FILTER (WHERE product_family = 'Flintstones' AND product = 'Rock 1') as "Flintstones | Rock1 | MAX(revenue)"
        , AVG(inventory) FILTER (WHERE product_family = 'Flintstones' AND product = 'Rock 1') as "Flintstones | Rock1 | AVG(inventory)"

        , MAX(revenue) FILTER (WHERE product_family = 'Flintstones' AND product = 'Rock 2') as "Flintstones | Rock2 | MAX(revenue)"
        , AVG(inventory) FILTER (WHERE product_family = 'Flintstones' AND product = 'Rock 2') as "Flintstones | Rock2 | AVG(inventory)"

        , MAX(revenue) FILTER (WHERE product_family = 'Jetsons' AND product = 'Laser 1') as "Jetsons | Laser1 | MAX(revenue)"
        , AVG(inventory) FILTER (WHERE product_family = 'Jetsons' AND product = 'Laser 1') as "Jetsons | Laser1 | AVG(inventory)"

        , MAX(revenue) FILTER (WHERE product_family = 'Jetsons' AND product = 'Laser 2') as "Jetsons | Laser2 | MAX(revenue)"
        , AVG(inventory) FILTER (WHERE product_family = 'Jetsons' AND product = 'Laser 2') as "Jetsons | Laser2 | AVG(inventory)"

    FROM my_table
    WHERE
        category in ('A','B')
        AND subcategory in ('Y','Z')
    GROUP BY
        ROLLUP(
              category
            , subcategory
        )
    ORDER BY
          category nulls last
        , subcategory nulls last
    `
    return run_query(db,sql)
}

async function build_pivot_sql(db,table_name,rows=undefined, columns=undefined, values=undefined, filters=undefined) {

    var sql = `
        with distinct_columns as (
            select
                distinct
                    product_family
                    , product
            from ` + table_name + 
        `)
        select * from distinct_columns
        `
    return run_query(db,sql)
}

function clean_query_parameter(parameter) {
    //Super basic - just prevent drops, inserts, updates, and semicolons.
    if (typeof parameter == 'number') return parameter;

    return parameter.replace(/;/gi,'')
                    .replace(/drop/gi, '')
                    .replace(/update/gi, '')
                    .replace(/insert/gi, '')
                    .replace(/delete/gi, '')
                    .replace(/alter/gi, '')
                    .replace(/create/gi, '');
}

async function run_query(db,sql,params) {
    if (typeof params == 'undefined') params = [];
    var outside_resolve, outside_reject;
    var db_promise = new Promise(function (resolve, reject) {
        outside_resolve = resolve;
        outside_reject = reject;
    });

    db.all(sql,...params, function(err, res) {
        if (err) outside_reject(err);
        outside_resolve(res);
    }); 

    return db_promise;
}

app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
})