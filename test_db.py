from app.database import run_query

result = run_query("select current_timestamp as now")
print("Connected! Time is:", result[0]['now'])