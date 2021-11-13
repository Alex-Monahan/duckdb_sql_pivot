window.onload = async function() {
    console.log('Loaded!!');

    // const response = await fetch('http:localhost:3000/pivot');

    // console.log(response);

    function reqListener () {
        console.log(this.responseText);
      }
      
      var oReq = new XMLHttpRequest();
      oReq.addEventListener("load", reqListener);
      oReq.open("GET", "http:localhost:3000/pivot?table_name=my_table");
      oReq.send();
};