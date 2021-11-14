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