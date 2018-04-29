var take_from_master = false;

const MASTER_IP = "localhost";

var tablet_ip = "localhost";

var port = "4567";

var BASE_URL = "ttp://localhost:4567/";

var ip_to_send = MASTER_IP;

var results_html = $(".results");


function request(data,endpoint)
{

    console.log(ip_to_send+":"+port+endpoint);
    // sendRequest(rows_object, "/client/set");
    $.post("http://"+ip_to_send+":"+port+endpoint ,JSON.stringify(data),function(response){

        var obj = JSON.parse(response);

        if(obj.tablet_IP != null)
        {
            ip_to_send = obj.tablet_IP;

            console.log("switch to tablet with ip : "+ ip_to_send);

            $.post("http://"+ip_to_send+":"+port+endpoint ,JSON.stringify(data),function(response) {

                var obj = JSON.parse(response);
                console.log(" back fromtablet with ip.. :" + ip_to_send);
                if (obj.tablet_locked != null)
                {
                    console.log(" tablet locked try again later..");
                }
            });

        }
        else if(obj.master_IP != null) {


            ip_to_send = obj.master_IP;
            console.log("switch to master with ip : " + ip_to_send);
            results_html.empty();
            results_html.append("switch to master with ip : "+ ip_to_send+"<br>");

            // send again to master to get data and forward it to user.
            $.post("http://" + ip_to_send + ":" + port+endpoint, JSON.stringify(data), function (response) {
                var obj = JSON.parse(response);
                ip_to_send = obj.tablet_IP;

                console.log(" switching to tablet with ip.. :" + ip_to_send+"\n");
                results_html.append("switch to tablet with ip : "+ ip_to_send+"<br>");

                $.post("http://" + ip_to_send + ":" + port+endpoint, JSON.stringify(data), function (response) {

                    var obj = JSON.parse(response);
                    console.log(" back fromtablet with ip.. :" + ip_to_send+"\n");
                    results_html.append("back to tablet with ip : "+ ip_to_send+"<br>");

                    if (obj.tablet_locked != null) {
                        results_html.empty();
                        results_html.append("locked tablet with ip : "+ ip_to_send+"<br>");

                        console.log(" tablet locked try again later..");

                    }
                });

            });

        }
        else{
            console.log(" successed operation : "+JSON.stringify(obj));

            $(".results").empty();
            $(".results").append("successed operation : \n "+JSON.stringify(obj));
        }
        console.log(JSON.stringify(data));
        console.log("set_row");

    });
}
// // $.post(BASE_URL+endpoint,JSON.stringify(data),function(response){
// //
// //         console.log("Response, ",  response);
// //
// //     }
// );



<!-- Adding row. -->


// Set row with n-columns.
$(".set .set_another_input").click(function(){

    $(".set .set_row_container").append("<div class = \"new_row\">"+
        "<label>country </label>"+
        "<input type= \"input\" name = \"country\">"+
        "<label>IPs </label>"+
        "<input type= \"input\" name = \"IPs\">"+
        "</div>");

});

$(".set_row").click(function() {

    var rows_object = []
    var domain = $(".set input[name=domain]").val();
    var i, $add_row_container = $(".set_row_container").children()
    for (i = 0; i < $add_row_container.length; i++) {

        var country = $add_row_container.eq(i).children("input[name=country]").val();
        var IPs = $add_row_container.eq(i).children("input[name=IPs]").val()
            .replace(/\s/g, '').split(',');
        rows_object.push({
            "domain_name": domain,
            "country": country,
            "IPs": IPs
        })
        // domain_name = $add_row_container.eq(i).children().first().children(".country").val();
        // domain_name = $add_row_container.eq(i).children().first().children(".country").val();
    }
    request(rows_object, "/client/set");
    // sendRequest(rows_object, "/client/set");
//     $.post("http://"+ip_to_send+":"+port ,JSON.stringify(rows_object),function(response){
//
//             var obj = JSON.parse(response);
//
//             if(obj.tablet_id != null)
//             {
//                 ip_to_send = obj.tablet_id;
//                 console.log("switch to tablet with ip : "+ ip_to_send);
//
//                 $.post("http://"+ip_to_send+":"+port ,JSON.stringify(rows_object),function(response) {
//
//                     var obj = JSON.parse(response);
//                     console.log(" back fromtablet with ip.. :" + ip_to_send);
//                     if (obj.tablet_locked != null)
//                     {
//                         console.log(" tablet locked try again later..");
//                     }
//                 });
//
//             }
//             else if(obj.master_id != null) {
//
//
//                 ip_to_send = obj.master_id;
//                 console.log("switch to master with ip : " + ip_to_send);
//
//                 // send again to master to get data and forward it to user.
//                 $.post("http://" + ip_to_send + ":" + port, JSON.stringify(rows_object), function (response) {
//                     var obj = JSON.parse(response);
//                     ip_to_send = obj.tablet_ip;
//
//                     console.log(" switching to tablet with ip.. :" + ip_to_send);
//
//                     $.post("http://" + ip_to_send + ":" + port, JSON.stringify(rows_object), function (response) {
//
//                         var obj = JSON.parse(response);
//                         console.log(" back fromtablet with ip.. :" + ip_to_send);
//                         if (obj.tablet_locked != null) {
//                             console.log(" tablet locked try again later..");
//                         }
//                     });
//
//                 });
//
//             }
//     console.log(JSON.stringify(rows_object));
//     console.log("set_row");
//
// });
});
/////
// Read row.
$(".read_row").click(function(){

    var domain = $(".read-row").children("input[name=domain]").val()
    var object = {
        domain_name : domain,
    }

    request(object, "/client/readrow");

    console.log(JSON.stringify(object));

});


// Delete row.
$(".delete_row").click(function(){

    var domain = $(".delete-row").children("input[name=domain]").val()
    var object = {
        domain_name : domain,
    }
    request(object, "/client/deleterow");
    console.log(JSON.stringify(object));

});


// Delete cells.
$(".delete_cells").click(function(){

    var domain = $(".delete-cells").children("input[name=domain]").val()
    var country = $(".delete-cells").children("input[name=country]").val()
    var object = {
        domain_name : domain,
        country : country
    }
    request(object, "/client/deletecells");

    console.log(JSON.stringify(object));

});
/////////////////////////////////
////////////////////////////////
// Append new input fields.
$(".add_another_input").click(function(){

    $(".add_row_container").append("<div class = \"new_row\">"+
        "<label>country </label>"+
        "<input type= \"input\" name = \"country\">"+
        "<label>IPs </label>"+
        "<input type= \"input\" name = \"IPs\">"+
        "</div>");

});

// When click addrows
$(".add_row").click(function(){

    var rows_object = []
    var domain = $(".add input[name=domain]").val();
    var i, $add_row_container = $(".add_row_container").children()
    for( i = 0;  i < $add_row_container.length; i++)
    {

        var country = $add_row_container.eq(i).children("input[name=country]").val();
        var IPs = $add_row_container.eq(i).children("input[name=IPs]").val()
            .replace(/\s/g, '').split(',');
        rows_object.push({
            "domain_name" : domain,
            "country" : country,
            "IPs" : IPs
        })
        // domain_name = $add_row_container.eq(i).children().first().children(".country").val();
        // domain_name = $add_row_container.eq(i).children().first().children(".country").val();
    }

    request(rows_object, "/client/addrow");

    console.log(JSON.stringify(rows_object));

});

