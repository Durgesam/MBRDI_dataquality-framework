$(document).ready(function() {
    document.getElementsByTagName("table")[0].setAttribute("id", "myTable");
    document.getElementsByTagName("tbody")[0].setAttribute("class", "mainBody");

    sortTable()

    $('#myTable tbody tr:has(td)').each(function() {
        var $this1 = $(this).find('td:eq(1)');
        var $this2 = $(this).find('td:eq(2)');
        var t = $this1.text();
        $this1.html(t.replace('&lt', '<').replace('&gt', '>'));
        var text = $this2.text();
        if (text == "Fail") {
          $(`<img src='html/images/failed.png' width="15" height="15" style="vertical-align:middle;margin:0px 20px">`).appendTo($($this2))

        }
        if (text == "Pass") {
          $(`<img src='html/images/passed.png' width="15" height="15" style="vertical-align:middle;margin:0px 15px">`).appendTo($($this2))

        }
    });

    $("#time_num").on("keyup", function() {
        filter_profile_row();
    });
    $("#profile").on("keyup", function() {
        filter_profile_row();
    });
    $("#result_timerange").on("change", function() {
        range_event();
    });
    $("#result").on("change", function() {
        filter_profile_row();
    });

    function range_event() {
        var e = document.getElementById("result_timerange");
        if (e.value == 'all') {
            document.getElementById("time_num").style.display = "none";
        } else {
            document.getElementById("time_num").style.display = "block";
        }
        filter_profile_row();
    }

    function filter_profile_row() {
        var time_type, time_num, num, profile_name, status, tr, td, i, time_col, profile_name_col, result_col, result,
        filtered, filtered_count;
        time_type = document.getElementById("result_timerange").value;
        time_num = document.getElementById("time_num");
        if (time_num.style.display == "block") {
            time_num = time_num.value;
            if (isValidNumber(time_num)) {
                if (time_num !== "") {
                    if (time_type == 'days') {
                        num = time_num;
                    } else if (time_type == 'weeks') {
                        num = time_num * 7;
                    } else if (time_type == 'months') {
                        num = time_num * 30;
                    } else if (time_type == 'years') {
                        num = time_num * 365;
                    }
                    var dt = new Date();
                    dt.setDate(dt.getDate() - num);
                    console.log("Current" + dt);
                }
            }
        }
        profile_name = document.getElementById("profile").value.toUpperCase();
        result = document.getElementById("result").value;
        status = document.getElementById("result").value.toUpperCase();
        filtered_count = 0;
        tr = document.getElementsByTagName("tr");
        for (i = 0; i < tr.length; i++) {
            td = tr[i].getElementsByTagName("td");
            if (td.length > 0) {
                time_col = td[0].innerHTML
                profile_name_col = td[1].textContent || td[1].innerText;
                result_col = td[2].textContent
                filtered = true;
                /* Status Filter */
                if (result != "all") {
                    if (result_col.toUpperCase().indexOf(status) <= -1) {
                        filtered = filtered && false;
                    }
                }
                /* Profile Name Filter */
                if (profile_name_col.toUpperCase().indexOf(profile_name) <= -1) {
                    filtered = filtered && false;
                }
                /* Time Range Filter */
                if (num != null) {
                    if (new Date(time_col) < dt) {
                        console.log(time_col);
                        filtered = filtered && false;
                    }
                }
                /* Show row if all conditions are true */
                if (filtered === true) {
                    tr[i].style.display = '';
                    filtered_count = filtered_count + 1
                } else {
                    tr[i].style.display = 'none';
                }
            }
        }
        change_row_count(filtered_count,result);
    }

    function change_row_count(count,status) {
        row_count = document.getElementById("count_rows");
        row_count.innerHTML = count;
        if (status == 'Pass') {
            row_count.style.color = 'green';
        } else if (status == 'Fail') {
            row_count.style.color = 'red';
        } else {
            row_count.style.color = 'blue';
        }
    }

    function isValidNumber(n) {
        if (isNaN(n)) {
            alert("Please enter Numeric value");
            return false;
        } else {
            return true;
        }
    }
    function sortTable() {
      var filterTable, rows, sorted, i, x, y, sortFlag;
      filterTable = document.querySelector("#myTable");
      sorted = true;
      while (sorted) {
         sorted = false;
         rows = filterTable.rows;
         for (i = 1; i < rows.length - 1; i++) {
            sortFlag = false;
            x = rows[i].getElementsByTagName("TD")[0];
            y = rows[i + 1].getElementsByTagName("TD")[0];
            if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
               sortFlag = true;
               break;
            }
         }
         if (sortFlag) {
            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
            sorted = true;
         }
      }
   }
});
