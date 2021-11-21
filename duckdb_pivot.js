import * as duckdb from './duckdb-browser.mjs';
import {dummy_build_columns, build_filter_list, refresh_grid, server_pivot_function} from './pivot_front_end.js'

(async () => {
    try {
        const DUCKDB_CONFIG = await duckdb.selectBundle({
            asyncDefault: {
                mainModule: './duckdb.wasm',
                mainWorker: './duckdb-browser.worker.js',
            },
            asyncNext: {
                mainModule: './duckdb-next.wasm',
                mainWorker: './duckdb-browser-next.worker.js',
            },
        });

        const logger = new duckdb.ConsoleLogger();
        const worker = new Worker(DUCKDB_CONFIG.mainWorker);
        const db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(DUCKDB_CONFIG.mainModule, DUCKDB_CONFIG.pthreadWorker);

        const conn = await db.connect();
        var test_output = await conn.query(`SELECT count(*)::INTEGER as v FROM generate_series(0, 100) t(v)`);
        console.log(test_output.toArray());
        await conn.close();
        await db.terminate();
        await worker.terminate();
    } catch (e) {
        console.error(e);
    }
})();
async function replicate_server_pivot() {
    // console.log('dummy_build_columns:',dummy_build_columns());
    const column_list = fetch('http://localhost:3000/column_list?table_name=my_table');

    column_list.then(async function(column_list_response) {build_filter_list(column_list_response,server_pivot_function);});
    var table_name = 'my_table';
    var filters = ''; //TODO
    var rows = ['category','subcategory'];
    var columns = ['product_family','product'];
    var values = ['MAX(revenue)','AVG(inventory)'];
    await refresh_grid(table_name,filters,rows,columns,values,undefined,server_pivot_function);
};
replicate_server_pivot();
async function client_pivot() {
    //Instantiate the DB and save it
    //Create dummy data
    //Pull the column list
    //Pass a function that will accept the DB connection and call the required queries against it

    // const column_list = 

}

async function duckdb_wasm_pivot(table_name,filters,rows,columns,values, row_subtotals=1) {
    console.log('Made it to duckdb_wasm_pivot!');
    return [{'test':'woot'}];
}