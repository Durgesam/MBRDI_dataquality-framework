$(document).ready(function() {
    document.getElementsByTagName("table")[0].setAttribute("id", "myTable");
    document.getElementsByTagName("tbody")[0].setAttribute("class", "mainBody");

    $('#myTable tbody tr:has(td)').each(function() {
        var $this1 = $(this).find('td:eq(0)');
        var $this2 = $(this).find('td:eq(4)');
        var t = $this1.text();
        $this1.html(t.replace('&lt', '<').replace('&gt', '>'));

        var text = $this2.text();
        if (text == "Fail") {
          $(`<img src='../../html/images/failed.png' width="15" height="15" style="vertical-align:middle;margin:0px 20px">`).appendTo($($this2))

        }
        if (text == "Pass") {
          $(`<img src='../../html/images/passed.png' width="15" height="15" style="vertical-align:middle;margin:0px 15px">`).appendTo($($this2))

        }
    });

    $("#result").on("change", function() {
        filter_profile_row();
    });

    function filter_profile_row() {

        var status, tr, td, i, filtered, result, result_col;

        result = document.getElementById("result").value;
        status = document.getElementById("result").value.toUpperCase();

        tr = document.getElementsByTagName("tr");
        for (i = 0; i < tr.length; i++) {

            td = tr[i].getElementsByTagName("td");
            if (td.length > 0) {
                result_col = td[4].textContent
                filtered = true;

                /* Status Filter */
                if (result != "all") {
                    if (result_col.toUpperCase().indexOf(status) <= -1) {
                        filtered = filtered && false;
                    }
                }
                if (filtered === true) {
                    tr[i].style.display = '';
                } else {
                    tr[i].style.display = 'none';
                }
            }
        }
    }

});