# Databricks notebook source
from pyspark.sql.functions import col, count, split

class SparkDFUnitTests:
  
  """
  This class is designed to perform various dataframe tests.
  
  Description of methods:
    duplicates_test - dataframe testing for duplicates
    nulls_test - dataframe testing for null values
    spaces_in_data - dataframe testing for spaces in rows in selected columns
    missing_values - matches the columns of the two dataframes and looks for lost values.
    The methods of application can be seen below
    
  Result returned by method: 1 or 0 + Description of problem if 0
  
  """
  
  def __init__(self, df):
    self.df = df
    
  def __repr__(self):
    return 'This class is designed to perform various dataframe tests.'
  
  def type_chek(cols):
    
    type_check_str = 0
    type_check_int = 0
    for a in cols:
      if type(a) is int:
        type_check_int += 1
      elif type(a) is str:
        type_check_str += 1
    if len(cols) == type_check_str:
      return False
    elif len(cols) == type_check_int:
      return True
    else:
      raise ValueError ("Invalid value type in columns list. Must be int or str. Ex [0,1,2,3] or ['column1', 'column2', 'column3']")
  
  def __make_needed_cols(df, cols):
    
    if SparkDFUnitTests.type_chek(cols):

      d = dict()
      ls = df.columns
      for ix, value in enumerate(ls):
        d[ix] = value

      ls = []
      for col in cols:
        cl = d.get(col)
        ls.append(cl)
        
    else:
      ls = cols
      
    return ls
  
  
  
  def duplicates_test(self, cols):

    columns_list = SparkDFUnitTests.__make_needed_cols(self.df, cols)
    
    dup_df = self.df.groupBy(columns_list).agg((count("*")).alias("frequency"))  #counting rows before and after dropping duplicates by primary key
    dup_df_all = dup_df[dup_df['frequency']>1]
    dup_counter = dup_df_all.count()

    if dup_counter != 0:
      dup_df[dup_df['frequency']>1].show()
      print(dup_counter)
      print ("Test failed.")
      test_duplicates = 0
    else:
      test_duplicates = 1
      print ("Test passed. Everything OK")
    return test_duplicates
  
  
  
  def nulls_test(self, cols):

    count_nulls = 0
    columns_list = SparkDFUnitTests.__make_needed_cols(self.df, cols)
    for cl in columns_list:
      test_nulls = self.df.where(col(cl).isNull()).count()
      if test_nulls != 0:
        count_nulls += 1
        print(f'There are {str(test_nulls)} Null values in {cl} column.')
        
    if count_nulls != 0:
      indicator_nulls = 0
      print('Test for nulls failed')
    else:
      indicator_nulls = 1
      print('Test for nulls passed')
    return indicator_nulls
  
  
  
  def spaces_in_data(self, cols):
    
    columns_list = SparkDFUnitTests.__make_needed_cols(self.df, cols)
    count_spaces = 0
    for cl in columns_list:
      test_begin = self.df.select(cl).withColumn('test_col', split(col(cl), ' ', 1)[0]).where(col('test_col') == ' ').count()
      test_end = self.df.select(cl).withColumn('test_col', split(col(cl), ' ', 1)[1]).where(col('test_col') == ' ').count()
      test_all = self.df.select(cl).withColumn('test_col', split(col(cl), ' ', 1)[0]).where(col('test_col') != col(cl)).count()
      if test_begin > 0:
        count_spaces += 1
        print(f'There are {str(test_begin)} rows with spaces in begin of row in {cl} column.')
      if test_end > 0:
        count_spaces += 1
        print(f'There are {str(test_end)} rows with spaces in end of row in {cl} column.')
      if test_all > 0:
        count_spaces += 1
        print(f'There are {str(test_all)} rows with spaces in {cl} column.')
    
    if count_spaces != 0:
      indicator_spaces = 0
      print('Test for spaces failed')
    else:
      indicator_spaces = 1
      print('Test for spaces passed')
    return indicator_spaces
  
  
  def missing_values(self, cols_left, df_right, cols_right):
    
    columns_list_left = SparkDFUnitTests.__make_needed_cols(self.df, cols_left)
    columns_list_right = SparkDFUnitTests.__make_needed_cols(df_right, cols_right)
    if len(columns_list_left) != len(columns_list_right):
      raise ValueError('[The number of columns in the left dataframe does not correspond to the number of columns in the right dataframe. It must be the same.]')
    count_missing = 0
    print(columns_list_left)
    for i in range(len(columns_list_left)):
      left_df = set([_[0] for _ in self.df.select(columns_list_left[i]).collect()])
      right_df = set([_[0] for _ in df_right.select(columns_list_right[i]).collect()])
      
      l = len(left_df)
      r = len(right_df)
      if r > l:
        diff = right_df - left_df
        count_missing += 1
        df_right.select(columns_list_right[i]).distinct().where(col(columns_list_right[i]).isin(diff)).display()
        print(f'There are {str(len(diff))} missing distinct values  in {columns_list_left[i]} column compared to the right. There are more distinct values in the right dataframe.')
      if l > r:
        diff = left_df - right_df
        count_missing += 1
        self.df.select(columns_list_left[i]).distinct().where(col(columns_list_left[i]).isin(diff)).display()
        print(f'There are {str(len(diff))} missing distinct values  in {columns_list_right[i]} column compared to the left. There are more distinct values in the left dataframe.')
        
    if count_missing != 0:
      indicator_missing = 0
      print('Test for missing values failed')
    else:
      indicator_missing = 1
      print('Test for missing values passed')
    return indicator_missing
      
  def empty_df(self):
    
    count_rows = self.df.count()

    if count_rows > 0:
      indicator_empty = 1
      print('Dataframe is not empty, everything OK.')
    else:
      indicator_empty = 0
      print('Dataframe is empty, please check.')
      
    return indicator_empty, count_rows

# COMMAND ----------

############ duplicates_test use example ############
# cols = [1,3,4] .... # List with cols ix, like [0,3,5], which are the key in the aggregate. In this test it will be primary key for duplicates_test
# t = SparkDFUnitTests(df_visits) # Making class object 
# test = t.duplicates_test(cols) # test results
############ duplicates_test use example ############

# COMMAND ----------

############ nulls_test use example ############
# cols = [1,3,4] .... # List with cols ix, like [0,3,5], which are the key. In this test, each of the specified columns will be checked for null value.
# t = SparkDFUnitTests(df_visits) # Making class object 
# test = t.nulls_test(cols) # test results
############ nulls_test use example ############

# COMMAND ----------

############ spaces_in_data use example ############
# cols = [1,3,4] .... # List with cols ix, like [0,3,5], which are the key in the aggregate. In this test, each of the specified columns will be checked for spaces in rows.
# t = SparkDFUnitTests(df_visits) # Making class object 
# test = t.spaces_in_data(cols) # test results
############ spaces_in_data use example ############

# COMMAND ----------

############ missing_values use example ############
# cols = [1,3,4] .... # List with cols ix, like [0,3,5], which are the key for main df (left). In this test, the matching will take place on a column-by-column basis. The number of columns submitted for comparison must be the same.
# cols_r = [2,5,6] .... # List with cols ix, like [0,3,5], which are the key for right df. In this test, the matching will take place on a column-by-column basis. The number of columns submitted for comparison must be the same.
# t = SparkDFUnitTests(df_visits) # Making class object 
# test = t.missing_values(
#                         cols, 
#                         df_right = df_r   -  Dataframe to which we are comparing
#                         cols_right = cols_r
#                         ) 
############ missing_values use example ############

# COMMAND ----------

