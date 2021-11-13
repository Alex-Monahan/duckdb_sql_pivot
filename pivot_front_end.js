window.onload = async function() {
    console.log('Loaded!!');

    const response = await fetch('http:localhost:3000/pivot?table_name=my_table');

    console.log(await response.json());

};