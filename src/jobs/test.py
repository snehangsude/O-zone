from datetime import datetime

a = "2012-09-23"
print(type(
    datetime.strptime(a, "%Y-%m-%d")
))