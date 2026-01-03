import pandas as pd


# HTML homepage template - Level 1
def html_template_homepage(dataframe):
    message = """
<html>
   <head>
      <link rel="stylesheet" type="text/css" href="html/css/style_homepage.css"/>
      <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.8.3/jquery.min.js"></script>
      <script src="html/js/main_homepage.js"></script>
   </head>
   <body>
      <div style="text-align: center;">
  <img src='html/images/DQF.png' width="500" height="100" onerror="this.style.display='none'"/>
</div>
<ul class="breadcrumb">
  <li><a href="#">Home</a></li>
</ul>
      <div class="result_heading">
         <h2>Overall Result</h2>
      </div>
      <div class="row">
         <div class="filter_status">
            <h2>Result Status</h2>
            <select class="cl_result" id="result" STYLE="background-color: #f5fcfc;">
               <option value="all">All </option>
               <option value="Pass">Pass</option>
               <option value="fail">Fail</option>
            </select>
         </div>
         <div class="filter_profile">
            <h2>Profile</h2>
            <input type="text" id="profile" name="num"  placeholder="Enter profile name..." STYLE="background-color: #f5fcfc;">
         </div>
         <div class="filter_range">
            <h2>Time Range</h2>
            <select id="result_timerange" STYLE="background-color: #f5fcfc;">
               <option value="all">All </option>
               <option value="days">Days</option>
               <option value="weeks">Weeks</option>
               <option value="months">Months</option>
               <option value="years">Years</option>
            </select>
            <input type="text" id="time_num" name="num" placeholder="Enter number..." STYLE="background-color: #f5fcfc;">
            <h2 id="count">Count: <span id="count_rows"></span></h2>
         </div>
      </div>
      </div>
      <div class="tableFixHead"> {table} </div>
   </body>
</html>"""

    data = dataframe
    data = data[["Time", "Config Name", "Overall Result"]]

    html = message.format(
        table=data.to_html(index=False))
    return html


# HTML Check run template - Level 2
def html_template_checkresult(dataframe, datetime):
    message = """
<html>
   <head>
      <link rel="stylesheet" type="text/css" href="../../html/css/style_checkruns.css"/>
      <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.8.3/jquery.min.js"></script>
      <script src="../../html/js/main_checkruns.js"></script>
   </head>
   <body>
      <div style="text-align: center;">
  <img src='../../html/images/DQF.png' width="500" height="100" onerror="this.style.display='none'"/>
</div>
</div><ul class="breadcrumb">
  <li><a href="../../index.html">Home</a></li>
<li><a href="#">Checkruns</a></li>
 
</ul>
      
      <div class="result_heading">
         <h2>Check Result</h2>
      </div>
      <div class="row">
         <div class="filter_status">
            <h2>Result Status</h2>
            <select class="cl_result" id="result" STYLE="background-color: #f5fcfc; width:200px;">
               <option value="all">All </option>
               <option value="Pass">Pass</option>
               <option value="fail">Fail</option>
            </select>
         </div>
         <div class="heading">
         <h2>
         {time_stamp}
         <h2>
      </div>
      </div>
      
      <div class="tableFixHead"> {table} </div>
   </body>
</html>"""

    data = dataframe[["Check_Name", "Table_Name", "Key", "Value", "Result"]]

    html = message.format(
        table=data.toPandas().to_html(index=False), time_stamp=datetime)
    return html


# HTML Failed data template - Level 3
def html_template_failed_data(dataframe, p, col, test_name, html_file):
    message = """
<html>
   <head>
      <link rel="stylesheet" type="text/css" href="../../../html/css/style_failed_data.css"/>
   </head>
   <body>
         <div style="text-align: center;">
  <img src='../../../html/images/DQF.png' width="500" height="100" onerror="this.style.display='none'"/>
</div>
</div><ul class="breadcrumb">
  <li><a href="../../../index.html">Home</a></li>
<li><a href="../{html_file}">Checkruns</a></li>
<li><a href="#">Failed Data</a></li>
 
</ul>
      <h2>Arguments</h2>
      <div class="tablesize">
         <div class="tableFixHead"> {table2} 
         </div>
      </div>
      <h2>Failed Data</h2>
      <h3> Total count of failed rows/values are {rows} </h3>
      <div class="tableFixHead"> {table1} 
      </div>
   </body>
   </body>
</html>"""

    p = [["check name", test_name], ["column(s)", col]] + p
    sub_dataframe = pd.DataFrame(p, columns=['Argument', 'Value'])
    data = dataframe
    row_count = data.count()
    data = data.limit(50)
    html = message.format(
        table1=data.toPandas().to_html(index=False), table2=sub_dataframe.to_html(index=False), rows=row_count, html_file=html_file)
    return html

