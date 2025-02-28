# Databricks notebook source
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import  Union


# COMMAND ----------

class DateGetter:
    """
    A class for manipulating datetime object and date ranges.
    
    Attributes:
    -----------
     - d_raises: dict
         A dictionary that contains the errors to be raised and their descriptions
    
    Methods:
    --------
    None:
        initializes the class with the provided date and/or format, used to format initialization string to datetime object
    
    - next_month(self, month_lag: int = 1) -> "DateGetter":
        moves the date value to the next month from n months from the present date.
    
    - next_day(self, day_lag: int = 1) -> "DateGetter":
        moves the date value to the next day n number of days from the present day.
        
    - next_week(self, weeks_lag: int = 1) -> "DateGetter":
        moves the date value to the next week from n weeks from the present date. 
    
    - prev_month(self, month_lag: int = 1) -> "DateGetter":
        moves the date value to the previous month from n months from the present date.
        
    - prev_day(self, day_lag: int = 1) -> "DateGetter":
        moves the date value to the previous day n number of days from the present day.  
    
    - prev_week(self, weeks_lag: int = 1) -> "DateGetter":
        moves the date value to the previous week from n weeks of the present date. 
    
    - set_day(self, new_day: int) -> "DateGetter":
        sets the day of the present date to a new day
        
    - set_month(self, new_month: int) -> "DateGetter":
        sets the month of the present date to a new month
        
    - set_year(self, new_year: int) -> "DateGetter":
        sets the year of the present date to a new year
        
    - month_start_date(self) -> "DateGetter":
        sets the present date to the start date of the present month
        
    - month_end_date(self) -> "DateGetter":
        sets the present date to the end date of the present month
    
    - week_start_date(self) -> "DateGetter":
        sets the present date to the start date of the present week
    
    - week_end_date(self) -> "DateGetter":
        sets the present date to the end date of the present week
    
    - quarter_start_date(self) -> "DateGetter":
        sets the present date to the start date of the present quarter
    
    - quarter_end_date(self) -> "DateGetter":
        sets the present date to the end date of the present quarter
    
    - fy_start_date(self) -> "DateGetter":
        sets the present date to the start date of the present FY
        
    - fy_end_date(self) -> "DateGetter":
        sets the present date to the end date of the present FY
    
    - calculate_quarter_num(self) -> int:
        calculates and returns the quarter number of the present date
    
    - get_string_quarter(self) -> str:
        returns the string representation of the present quarter
    
    - date_range(self, dt_start: Union[str, datetime.date, None] = None, dt_end: Union[str, datetime.date, None] = None,
                 step: int = 1, step_type: str = "day", str_format: Union[str, None] = None,
                 include_last_date: bool = False, convert_result_to_str: bool = False) -> list:
        returns the list of days within the date range with the given parameters
    
    - date_format_to_string(self, date_format: Union[str, None] = None) -> str:
        formats the present date object to a specific string format
    """
    
    d_raises = {
        "missed_date_format": "Error in string formatting,"
        " format for conversion not passed, please pass correct format, e.g. < '%Y-%m-%d'>.",
        "missed_range_end_date": "Error in date range creating," 
        " please provide dt_end parameter in datetime.date type or str type with format to convert.",
        "missed_range_start_date": "Error in date range creating,"
        " please provide dt_start parameter in datetime.date type or str type with format to convert.",
    }

    def __init__(
        self,
        initial_date: Union[datetime.date, str] = date.today(),
        format: Union[str, None] = None,
    ) -> None:
        """
        Initializes the DateGetter class.
        
        Parameters:
        ----------
         - initial_date: Union[datetime.date, str]
             A datetime date object or string for the base date used to manipulate date range.
            
         - format: Union[str, None]
             The specified format for string to datetime object conversion, None by default
            
        Returns:
        -------
        None
        """
        self.date_calc = initial_date
        self.format = format
        
        if type(self.date_calc) == str:
            if self.format is None:
                raise TypeError(self.d_raises["missed_date_format"])
                
            self.date_calc = datetime.strptime(self.date_calc, self.format)

    def next_month(self, month_lag: int = 1) -> "DateGetter":
        """
        Move the date value to the next month from n months from the present date.
        
        Parameters:
        ----------
         - month_lag: int
             The integer value to be used for date range manipulation
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc += relativedelta(months=month_lag)
        return self

    def next_day(self, day_lag: int = 1) -> "DateGetter":
        """
        Move the date value to the next day n number of days from the present day.
        
        Parameters:
        ----------
         - day_lag: int
             The integer value to be used for date range manipulation
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc += relativedelta(days=day_lag)
        return self

    def next_week(self, weeks_lag: int = 1) -> "DateGetter":
        """
        Move the date value to the next week from n weeks from the present date.
        
        Parameters:
        ----------
         - weeks_lag: int
             The integer value to be used for date range manipulation
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc += relativedelta(weeks=weeks_lag)
        return self

    def prev_month(self, month_lag: int = 1) -> "DateGetter":
        """
        Move the date value to the previous month from n months from the present date.
        
        Parameters:
        ----------
         - month_lag: int
             The integer value to be used for date range manipulation
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc -= relativedelta(months=month_lag)
        return self

    def prev_day(self, day_lag: int = 1) -> "DateGetter":
        """
        Move the date value to the previous day n number of days from the present day.
        
        Parameters:
        ----------
         - day_lag: int
             The integer value to be used for date range manipulation
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc -= relativedelta(days=day_lag)
        return self

    def prev_week(self, weeks_lag: int = 1) -> "DateGetter":
        """
        Move the date value to the previous week from n weeks of the present date.
        
        Parameters:
        ----------
         - weeks_lag: int
             The integer value to be used for date range manipulation
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc -= relativedelta(weeks=weeks_lag)
        return self

    def set_day(self, new_day: int) -> "DateGetter":
        """
        Sets the present date to a new day
        
        Parameters:
        ----------
         - new_day: int
             The integer value to be used for representing the new day
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.date_calc.replace(day=new_day)
        return self

    def set_month(self, new_month: int) -> "DateGetter":
        """
        Sets the present date to a new month
        
        Parameters:
        ----------
         - new_month: int
             The integer value to be used for representing the new month
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.date_calc.replace(month=new_month)
        return self

    def set_year(self, new_year: int) -> "DateGetter":
        """
        Sets the present date to a new year
        
        Parameters:
        ----------
         - new_year: int
             The integer value to be used for representing the new year
            
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.date_calc.replace(year=new_year)
        return self

    @property
    def month_start_date(self) -> "DateGetter":
        """
        Sets the present date to the start date of the present month.
        
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.date_calc.replace(day=1)
        return self

    @property
    def month_end_date(self) -> "DateGetter":
        """
        Sets the present date to the end date of the present month.
        
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.next_month().month_start_date.date_calc - relativedelta(
            days=1
        )
        return self

    @property
    def week_start_date(self) -> "DateGetter":
        """
        Sets the present date to the start date of the present week.
        
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.date_calc - timedelta(days=self.date_calc.weekday())
        return self

    @property
    def week_end_date(self) -> "DateGetter":
        """
        Sets the present date to the end date of the present week.
        
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.week_start_date.date_calc + timedelta(days=6)
        return self

    @property
    def quarter_start_date(self) -> "DateGetter":
        """
        Sets the present date to the start date of the present quarter.
        
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = date(
            self.date_calc.year, 3 * self.calculate_quarter_num - 2, 1
        )
        return self

    @property
    def quarter_end_date(self) -> "DateGetter":
        """
        Sets the present date to the end date of the present quarter.
        
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.quarter_start_date.date_calc + relativedelta(
            months=3, days=-1
        )
        return self

    @property
    def fy_start_date(self) -> "DateGetter":
        """
        Sets the present date to the start date of the present Fiscal Year.
        
        Returns:
        -------
        DateGetter object
        """
        fy_month = 7
        
        if self.date_calc.month < fy_month:
            self.date_calc = date(self.date_calc.year - 1, fy_month, 1)
        else:
            self.date_calc = date(self.date_calc.year, fy_month, 1)
        return self

    @property
    def fy_end_date(self) -> "DateGetter":
        """
        Sets the present date to the end date of the present Fiscal Year.
        
        Returns:
        -------
        DateGetter object
        """
        self.date_calc = self.fy_start_date.date_calc + relativedelta(years=1, days=-1)
        return self

    @property
    def calculate_quarter_num(self) -> int:
        """
        Calculates the quarter number of present date.
        
        Returns:
        -------
        int
        """
        return (self.date_calc.month - 1) // 3 + 1

    @property
    def get_string_quarter(self) -> str:
        """
        Returns the string representation of the present quarter.
        
        Returns:
        -------
        str
        """
        quarters_map_dict = {1: "JFM", 2: "AMJ", 3: "JAS", 4: "OND"}
        return quarters_map_dict[self.calculate_quarter_num]

    def date_range(
        self,
        dt_start: Union[str, datetime.date, None] = None,
        dt_end: Union[str, datetime.date, None] = None,
        step: int = 1,
        step_type: str = "day",
        str_format: Union[str, None] = None,
        include_last_date: bool = False,
        convert_result_to_str: bool = False,
    ) -> list:
        """
        Returns the list of days within the date range with the given parameters
        
        Parameters:
        ----------
         - dt_start: Union[str, datetime.date, None] = None
             The starting date of the date range, default set to present date
            
         - dt_end: Union[str, datetime.date, None] = None
             The end date of the date range
            
         - step: int = 1
             The integer step value with which to generate the list
            
         - step_type: str = "day"
             The time unit value to step by date range
            
         - str_format: Union[str, None] = None
             The format of the resulting date string, default set to object's format
            
         - include_last_date: bool = False
             A boolean type to indicate whether or not to include end date to the date range
            
         - convert_result_to_str: bool = False
             A boolean type to indicate whether or not to convert the resulting list of days to string format
            
        Returns:
        -------
        list
        """
        dt_start = dt_start or self.date_calc
        str_format = str_format or self.format
        step = abs(step)
        
        d_steps = {
            "day": relativedelta(days=step),
            "month": relativedelta(months=step),
            "week": relativedelta(weeks=step),
            "year": relativedelta(years=step),
        }
        
        def date_check(date_to_check, message_key: str):
            if isinstance(date_to_check, str) and str_format is not None:
                date_to_check = datetime.strptime(date_to_check, str_format)
            if date_to_check is None:
                raise ValueError(self.d_raises[message_key])
            elif not isinstance(date_to_check, date):
                raise ValueError(self.d_raises[message_key])
            return date_to_check
        
        dt_start = date_check(dt_start, "missed_range_start_date")
        dt_end = date_check(dt_end, "missed_range_end_date")

        date_initial = dt_start

        dates_generated = []
        while date_initial <= dt_end:
            dates_generated.append(date_initial)
            date_initial += d_steps[step_type]

        if include_last_date and dt_end not in dates_generated:
            dates_generated.append(dt_end)

        if str_format is not None and convert_result_to_str:
            dates_generated = [
                DateGetter(_).date_format_to_string(str_format) for _ in dates_generated
            ]

        return dates_generated

    def date_format_to_string(self, date_format: Union[str, None] = None) -> str:
        """
        Formats the present date object to a specific string format.
        
        Parameters:
        ----------
         - date_format: Union[str, None] = None
             The format of the date string to be returned, default set to object's format
            
        Returns:
        -------
        str
        """
        date_format = date_format or self.format
        
        if date_format is None:
            raise TypeError(self.d_raises["missed_date_format"])
            
        d_new_formats = {
            "%QAR": self.get_string_quarter,
            "%QNUM": self.calculate_quarter_num,
        }
        
        for nfc, nfr in d_new_formats.items():
            date_format = date_format.replace(nfc, str(nfr))
            
        return self.date_calc.strftime(date_format)
