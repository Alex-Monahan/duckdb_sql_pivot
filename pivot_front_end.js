/* Front End TODO list:
        Make column headers dynamic
        Make basic draggable/droppable GUI
        Test out a lot of combinations
        Order by / filter each column
        Add GUI and support for other parameters (Ex: subtotals or not, subtotals above or below (just ASC vs. DESC in GROUPING() in ORDER BY clause))
        Freeze panes
        Auto-size the header widths
        Rowspan?
        Allow for Values sigma feature?
            If you do it on the front end, should be able to just re-order the column headers 
            (as long as original name is tracked separately for the prop value)
        

*/
window.onload = async function() {

    
    console.log('Loaded!!');
    const column_separator = ' | ';
    const response = await fetch('http:localhost:3000/pivot?table_name=my_table&rows=category,subcategory&columns=product_family,product&values=MAX(revenue),AVG(inventory)');

    var pivoted_data = await response.json();

    const grid = document.querySelector('revo-grid');
    grid.resize = true;
    grid.autoSizeColumn = {
        mode: 'autoSizeAll',
        allColumns: true,
        preciseSize: true
    };
    const columns = [
        { prop: 'name', name: 'First column' }
    ];
    
    var dummy_var = build_columns(pivoted_data,column_separator);
    grid.columns = dummy_build_columns();
    grid.source = pivoted_data;


    // function build_columns(pivoted_data, column_separator) {
    //     var output_columns = [];
    //     var sql_headers = [];
    //     for (var column in pivoted_data[0]) {
    //         sql_headers.push(column);
    //     }

    //     var split_sql_headers = [];
    //     for (var i=0; i < sql_headers.length; i++) {
    //         split_sql_headers.push(sql_headers[i].split(column_separator));
    //     }

    //     console.log('split_sql_headers:\n',split_sql_headers);

    //     var output_object = {};
    //     // var piece_of_output;
    //     for (var i=0; i < split_sql_headers.length; i++) {
    //         for (depth=0; depth < max_depth; depth++) {
                
    //         }
        
    //     }
    //     console.log(output_object);
    // }


    function OLD_build_columns(pivoted_data,column_separator) {
        var columns = [];
        for (var column in pivoted_data[0]) {
            nested_columns = column.split(column_separator);
            if (nested_columns.length == 1) {
                columns.push({prop:column, name:column});
            } else {
                
            }
            
        }
        return columns;
    }

    function build_columns(pivoted_data, column_separator) {
        var output_columns = [];
        var sql_headers = [];
        for (var column in pivoted_data[0]) {
            sql_headers.push(column);
        }

        var split_sql_headers = [];
        for (var i=0; i < sql_headers.length; i++) {
            split_sql_headers.push(sql_headers[i].split(column_separator));
        }

        console.log('split_sql_headers:\n',split_sql_headers);
        //Because we are always doing all combinations of things, I know the depth will be consistent for every column that has a separator
        //(headers in the "rows" parameter don't have a separator so they have a depth of 1)
        //I also know that as soon as I find a column with a depth > 1, all other columns will have that depth
        // var max_depth = 0;
        // for (var i=0; i < split_sql_headers.length; i++) {
        //     if (split_sql_headers[i].length > max_depth) max_depth = split_sql_headers[i].length;
        // }
        // console.log('max_depth',max_depth);
        var max_depth;
        var depth;
        var prior_names = []; //An array with the same length as max_depth. Will contain the most recent names.
        var nested_column_to_add;
        var current_level;

        var delta_found = false;
        for (var i=0; i < split_sql_headers.length; i++) {
            delta_found = false
            max_depth = split_sql_headers[i].length;
            if (max_depth == 1) {
                output_columns.push({name:sql_headers[i],prop:sql_headers[i]})
                continue;
            }
            //Initialize the first depth > 1 node's initial value
            // console.log(output_columns.at(-1));
            // if (typeof output_columns.at(-1).children == 'undefined') {
            //     output_columns.push({name:split_sql_headers[i][0],children:[]});
            // }
            for (depth=0; depth < max_depth; depth++) {
                console.log('split_sql_headers[i][depth]',split_sql_headers[i][depth]);
                
                if (depth == 0) {
                    // current_level = output_columns.at(-1);
                    current_level = output_columns;
                }
                console.log('output_columns',output_columns);
                console.log('current_level',current_level);
                if (current_level.at(-1)?.name != split_sql_headers[i][depth]) {
                    if (depth == (max_depth - 1)) {
                        current_level.push({name:split_sql_headers[i][depth], prop:sql_headers[i]});
                        break;
                    } else {
                        current_level.push({name:split_sql_headers[i][depth], children: []});
                    }  
                }
                current_level = current_level.at(-1).children;

                // current_children = current_children[-1].children;
                // if (typeof current_children == 'undefined') current_children = [];
                // if (depth = max_depth - 1) {
                //     // nested_column_to_add
                //     current_children.push({name:split_sql_headers[i][depth], prop:sql_headers[i]})
                //     break;
                // }
                // if (split_sql_headers[i][depth] != prior_names[depth]) {
                    
                //     current_children = [];
                //     // nested_column_to_add = {name:};
                    
                //     nested_column_to_add['name'] = split_sql_headers[i][depth];

                // }

            }
            // for (depth=0; depth < max_depth; depth++) {
            //     prior_names[depth] = split_sql_headers[i][depth];
            // }


        }

        console.log(output_columns);
        return output_columns;

    }
    // function build_columns(pivoted_data, column_separator) {
    //     var output_columns = [];
    //     var sql_headers = [];
    //     for (var column in pivoted_data[0]) {
    //         sql_headers.push(column);
    //     }

    //     var split_sql_headers = [];
    //     for (var i=0; i < sql_headers.length; i++) {
    //         split_sql_headers.push(sql_headers[i].split(column_separator));
    //     }

    //     console.log('split_sql_headers:\n',split_sql_headers);
    //     var current_children = [];
    //     output_columns = build_columns_recursive(sql_headers, split_sql_headers, 0, 0, output_columns, current_children);

    // }
    // function build_columns_recursive(sql_headers, split_sql_headers, column_number, depth, output_columns, current_children) {
    //     var max_depth = split_sql_headers[column_number].length;

    //     if (depth == max_depth -1) {
    //         current_children.push({name:split_sql_headers[column_number][depth], prop:sql_headers[i]})
    //         return current_children;
    //     }

    //     // output_columns['children'] = current_children;

    //     return output_columns;
    // }

    function dummy_build_columns() {
        column_size = 125;
        //Maybe I can build my own size by taking the length of the deepest node's name and multiplying by 7px?
        //Can I do this with a loop instead of recursively? I can just look back and see if we are at the final depth and if it has changed since the last one?
        /*
            for column in columns:
                for j in depth:
                    if column_part 1 != prior_column_part_1 then create a name:'column_part_1' with everything up to this point?

        */
        return [
            {name:'category', prop: 'category'},
            {name:'subcategory', prop: 'subcategory'},
            {
                name: 'Flintstones', children: [
                    {name: 'Rock 1', children: [
                        {name: 'AVG(inventory)', prop: 'Flintstones | Rock 1 | AVG(inventory)', size:column_size},
                        {name: 'MAX(revenue)', prop: 'Flintstones | Rock 1 | MAX(revenue)', size:column_size}
                    ]},
                    {name: 'Rock 2', children: [
                        {name: 'AVG(inventory)', prop: 'Flintstones | Rock 2 | AVG(inventory)', size:column_size},
                        {name: 'MAX(revenue)', prop: 'Flintstones | Rock 2 | MAX(revenue)', size:column_size}
                    ]}
                ]
            },
            {
                name: 'Jetsons', children: [
                    {name: 'Laser 1', children: [
                        {name: 'AVG(inventory)', prop: 'Jetsons | Laser 1 | AVG(inventory)', size:column_size},
                        {name: 'MAX(revenue)', prop: 'Jetsons | Laser 1 | MAX(revenue)', size:column_size}
                    ]},
                    {name: 'Laser 2', children: [
                        {name: 'AVG(inventory)', prop: 'Jetsons | Laser 2 | AVG(inventory)', size:column_size},
                        {name: 'MAX(revenue)', prop: 'Jetsons | Laser 2 | MAX(revenue)', size:column_size}
                    ]}
                ]
            }
        ]

    }
};