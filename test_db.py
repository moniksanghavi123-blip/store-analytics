from app.database import run_query


def main():
    result = run_query("select current_timestamp as now")
    print("Connected! Time is:", result[0]["now"])


if __name__ == "__main__":
    main()
