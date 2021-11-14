/* Front End TODO list:
        Make column headers dynamic
        Make basic draggable/droppable GUI
        Test out a lot of combinations
        Order by / filter each column
        Add GUI and support for other parameters (Ex: subtotals or not)
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
    
    // grid.columns = build_columns(pivoted_data,column_separator);
    grid.columns = dummy_build_columns();
    grid.source = pivoted_data;


    // function build_columns(pivoted_data,column_separator) {
    //     var columns = [];
    //     for (var column in pivoted_data[0]) {
    //         nested_columns = column.split(column_separator);
    //         if (nested_columns.length == 1) {
    //             columns.push({prop:column, name:column});
    //         } else {
                
    //         }
            
    //     }
    //     return columns;
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