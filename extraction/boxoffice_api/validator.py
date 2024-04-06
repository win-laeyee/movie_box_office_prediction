from datetime import datetime as dt


class Validator:
    def __init__(self):
        self.is_valued = False
        self.result = None

    def check_weekly(self, year: int, week: str):
        try:
            week = int(week)
            year = int(year)
            current_year = int(dt.now().year)
            if year < 1982:
                raise ValueError("We Just Have Information after 1982 in the database")
            if week > 52:
                raise ValueError("week must be less than 52")
            if int(dt.now().year) < year:
                raise ValueError("Inserted Year is Invalid")
            if current_year == year and int(dt.now().strftime("%U")) - 1 < week:
                raise ValueError("Provided week is Out of range")

            else:
                self.is_valued = True
                return self.is_valued

        except Validator as e:
            self.is_valued = False
            print(f"Input type Error : {e}")


    def check_integer(self, value: int):
        return isinstance(value, int)

    def check_year(self, year: int):
        if self.check_integer(value=year):
            return 1982 < year < int(dt.now().year)
        else:
            print("Year Must be an Integer")

    def check_month(self, month: int):
        if self.check_integer(value=month):
            return 0 < month < 13
        else:
            print("Month Must be an Integer")

    def check_week(self, week: str):
        if self.check_integer(value=int(week)):
            return 0 < int(week) < 53
