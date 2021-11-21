import * as duckdb from './duckdb-browser.mjs';
import {dummy_build_columns} from './pivot_front_end.js'

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
(async () => {
    
    console.log('dummy_build_columns:',dummy_build_columns());
})();
async function duckdb_wasm_pivot(table_name,filters,rows,columns,values, row_subtotals=1) {
    console.log('Made it to duckdb_wasm_pivot!');
    return [{'test':'woot'}];
}