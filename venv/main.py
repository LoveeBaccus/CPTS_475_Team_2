import pandas as pd
import numpy as np
from datetime import datetime 

from numpy import mean
from numpy import std

# TO DO 
# Verify prepIndividualData() is doing what it needs to
  # decide what to do about multiple timestamps
# Clean the data so incomplete values are discarded
  # use the first pos date
  # drop data points that aren't AQI
  # talk about AQI in report 
  # we could total all the AQI
  # summarize the data for the hour -- give an overview of the days 
  # summarize by days of the week 
  # hit map with days vs weeks  
# join individual data into one large dataframe
# analyze the data in some interesting way -> create a visualization


# accepts the student name, and vectors containing the filepaths for all the necessary csv files
# appends the multiple pos and meas files
# joins the Pos and Meas DFs into one
# currently leaves both timestamps in the DF, we need to decide what we want to do with those
def prepIndividualData(studentName, posFilePaths, measFilePaths):
    posDF = pd.DataFrame() # initialize empty dataframes
    updatedPosDF = pd.DataFrame()
    measDF = pd.DataFrame()

    # append all the position files
    fileNum = 1
    for file in posFilePaths:
        if fileNum == 1:
            posDF = pd.read_csv(file)
        else:
            tempDF = pd.read_csv(file)
            posDF.append(tempDF)
        fileNum = fileNum + 1

    # append all measure files
    fileNum = 1
    for file in measFilePaths:
        if fileNum == 1:
            measDF = pd.read_csv(file)
            #print('fileNum = ', fileNum)
            #print(measDF.head(2))

        else:
            tempDF = pd.read_csv(file)
            #print('*******************************************************')
            #print('fileNum = ', fileNum)
            #print(tempDF.head(2))
            measDF.append(tempDF)
        fileNum = fileNum + 1

    

    # We need to get rid of the multiple measurements for one minute in PosDF, so we are seperating the date and time column so we can delete the seconds feild   
    # add a new column that zeros out the seconds so we can use dedupe to remove multiple data points from the same minute 
    # dedupe() on the new column
    posDF[['Calendar Date', 'Clock Time']] = posDF['date'].str.split(expand = True)
    posDF[['Hours','Minutes','Seconds']] = posDF['Clock Time'].str.split(pat=':', expand=True)

    # mushing the hours and minutes back, but leaving the seconds behind
    posDF['New Time'] = posDF['Hours'].map(str) + ':' + posDF['Minutes'].map(str) + ':00'

    # mushing the Calendar Date and the New Time
    posDF['Pos Date'] = posDF['Calendar Date'].map(str) + ' ' + posDF['New Time'].map(str)
    
    posDF.to_csv('testOutput', index = True)
    # create a new Data Frame for the features we actually want in the final
    updatedPosDF = posDF[['timestamp', 'Pos Date', 'latitude','longitude']]

    # remove the duplicate times in Pos Time
    updatedPosDF = updatedPosDF.drop_duplicates(subset='Pos Date')
    #L_data.to_csv('/content/gdrive/My Drive/CPTS 475 Group 2/TestOutput.csv', index = True)
    updatedPosDF.to_csv('UpdatedPosDF.csv', index = True)
    

    # if the number of elements is equal, then join the position and measurement DF in a new one
    # else we have a problem :/
    #df['Dates'] = pd.to_datetime(df['Dates'])
    #test = datetime.strptime("2022-10-31 21:12:00","%Y-%m-%d %H:%M:%S")

    updatedPosDF['Pos Date'] = pd.to_datetime(updatedPosDF['Pos Date'])
    measDF['date (UTC)'] = pd.to_datetime(measDF['date (UTC)'])

    updatedPosDF.rename(columns={'Pos Date':'DTS'}, inplace=True)
    measDF.rename(columns={'date (UTC)':'DTS'}, inplace=True)
    measDF = measDF.sort_values(by=['DTS'], ascending=True)

    studentDF = pd.merge_asof(measDF,updatedPosDF, on='DTS', direction= 'nearest')
    #studentDF.rename(columns={"","Index"})

    del studentDF["timestamp_x"]
    del studentDF["timestamp_y"]
    # add the student name 
    studentDF['Student'] = studentName

    #print(studentDF[0][0])

    return studentDF


if __name__ == "__main__":

    # This is where we can have a different file path that holds the correct path that would be on Marisa and Trey's google drive
    # we could layer this inside one more function that lets us choose between the two options so we don't have to remember to swap vectors 
    
    # generate vectors of file paths
    #testDataFrame = pd.read_csv(r"venv\L_Pos_1.csv")

    L_Pos_FilePath = ["venv\L_Pos_1.csv"]
    L_Meas_FilePath = ["venv\L_Measures_1.csv","venv\L_Measures_2.csv","venv\L_Measures_3.csv"]
    
    T_Pos_FilePath = ["venv\T_Pos.csv"]
    T_Meas_FilePath = ["venv\T_Measures.csv"]
    
    # prep individual data
    L_data = prepIndividualData("Lovee", L_Pos_FilePath, L_Meas_FilePath)
    T_data = prepIndividualData("Trey", T_Pos_FilePath, T_Meas_FilePath)
    # M_data =  # placeholder again

    # concat individual data into one mega dataframe
    # preppedData = pd.concat([L_data, T_data, M_data])
    
    # Testing 
    #print("##############################################")
    #print(L_data.head(4))
    L_data.to_csv('TestOutput.csv', index = True)
    # T_data.to_csv('TestOutput2.csv', index = True)
    print(L_data.head(10))