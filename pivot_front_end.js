window.onload = async function() {
    console.log('Loaded!!');

    const response = await fetch('http:localhost:3000/pivot?table_name=my_table');

    var pivoted_data = await response.json();

    const grid = document.querySelector('revo-grid');
    grid.resize = true;
    const columns = [
        { prop: 'name', name: 'First column' }
    ];
    
    grid.columns = build_columns(pivoted_data);
    grid.source = pivoted_data;


    function build_columns(pivoted_data) {
        var columns = [];
        for (var column in pivoted_data[0]) {
            columns.push({prop:column, name:column});
        }
        return columns;
    }
};