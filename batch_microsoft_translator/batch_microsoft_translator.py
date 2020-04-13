#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Documentation

"""
import os
import sys
import pandas as pd
import numpy as np
import csv
import time
import requests
import uuid
from io import StringIO
import json

class Translate_App():
    """

    Translate App
    =============
    The Translate_App class contains all the functions to run the translation app
    
    """
    retry_counter = 0

    def __init__(self, input_dict, **kwargs):
        self.validate_translate_app(input_dict)
 

    def validate_translate_app(self, input_dict):

        """

        All processes in py_gfkdata require self to be created and validated.

        """
        self.ERROR = []
        self.LOG = []
        self.run_type = "translate"
        self.remove_autodetect = False
        self.input_df_or_path_or_buffer = input_dict["input_df_or_path_or_buffer"]
        self.output_file_path = input_dict["output_file_path"]

        if "translate_cols_input" in input_dict:
            if "," in input_dict["translate_cols_input"]:
                input_dict["translate_cols_input"] = input_dict["translate_cols_input"].replace(",",";")
            if "|" in input_dict["translate_cols_input"]:
                input_dict["translate_cols_input"] = input_dict["translate_cols_input"].replace("|",";")
            self.cols_list = input_dict["translate_cols_input"].split(";")
        else:
            err = "the parameter 'translate_cols_input' is not present, it is required for translate process"
            self.ERROR.append(err)

        if "delimiter" in input_dict:
            self.delimiter = self.delimiter_validation(input_dict["delimiter"], raise_error=False)
        if not hasattr(self,"delimiter"):
            self.delimiter = "\t"

        if "subscriptionKey" in input_dict:
            self.subscriptionKey = input_dict["subscriptionKey"]
        else:
            err = "the parameter 'subscriptionKey' is not present, it is required for translate process"
            self.ERROR.append(err)

        if "tolang" in input_dict:
            self.tolang = input_dict["tolang"]
        else:
            err = "the parameter 'tolang' is not present, it is required for translate process"
            self.ERROR.append(err)

        self.fromlang = "autodetect"
        if "fromlang" in input_dict:
            if input_dict["fromlang"]:
                self.fromlang = input_dict["fromlang"]
        if self.fromlang == "autodetect":
            self.fromlang = None

        if self.fromlang:
            self.fromlang = input_dict["fromlang"]

        self.url = self.get_constructed_url(self.tolang, self.fromlang)
        
        self.create_id = False

        if not "id_variable" in input_dict:
            err = "the parameter 'id_variable' is not present, it is required for translate process"
            self.ERROR.append(err)

        if "id_variable" in input_dict:
            if input_dict["id_variable"]:
                self.id_variable = input_dict["id_variable"]
                self.create_id = False

        if "translated_suffix" in input_dict:
            if input_dict["translated_suffix"]:
                self.translated_suffix = input_dict["translated_suffix"]
        if not hasattr(self, "translated_suffix"):
            self.translated_suffix = "_Translated"

        self.batch_translate_cols = []
        self.dup_translate_cols = []
        self.error_response = {"response" : ""}

        self.translate_to_cols = []
        for col in self.cols_list:
            self.translate_to_cols.append("{0}{1}".format(col, self.translated_suffix))

        self.source_translated_dfs = {}
        self.to_translate_dfs = {}
        self.translated_dfs = {}

        for idx, col in enumerate(self.cols_list):
            translate_col = self.translate_to_cols[idx]
            cols = [self.id_variable, col, translate_col]
            self.source_translated_dfs[col] = pd.DataFrame(columns=cols)
            self.to_translate_dfs[col] = pd.DataFrame(columns=cols)
            self.translated_dfs[col] = pd.DataFrame(columns=cols)
 
        self.capitalize = True
        if "capitalize" in input_dict:
            if type(input_dict["capitalize"]) == bool:
                self.capitalize = input_dict["capitalize"]
        

        self.dtypes = {}
        if "dtypes" in input_dict:
            if input_dict["dtypes"]:
                for col, dtype in input_dict["dtypes"].items():
                    self.dtypes[col] = dtype
        if not self.id_variable in self.dtypes:
            self.dtypes[self.id_variable] = str
        for col in self.cols_list:
            self.dtypes[col] = str
        for col in self.translate_to_cols:
            self.dtypes[col] = str
        self.dtypes["Translated_ID"] = self.dtypes[self.id_variable]


    def translate_app_main(self):
        """

        Runs the translate app

        """
        if type(self.input_df_or_path_or_buffer) == str:
            self.data_type = "flat"
            
            print("Input data file {}".format(self.input_df_or_path_or_buffer))
            try:
                self.input_df = pd.read_csv(self.input_df_or_path_or_buffer, sep=self.delimiter,header=0)
            except (IOError,NameError,Exception) as e:
                raise Exception("ERROR: Could not read the file: {0} {1} {2}".format(self.input_df_or_path_or_buffer,e,sys.stderr))
                
        elif type(self.input_df_or_path_or_buffer) == pd.DataFrame:
            self.data_type = "df"
            self.input_df = self.input_df_or_path_or_buffer.copy()
        elif type(self.input_df_or_path_or_buffer) == StringIO():
            try:
                self.input_df = pd.read_csv(self.input_df_or_path_or_buffer, sep=self.delimiter,header=0)
            except (IOError,NameError,Exception) as e:
                 raise Exception("ERROR: Could not read the buffer: {0} {1} {2}".format(self.input_df_or_path_or_buffer,e,sys.stderr))
                
            self.data_type = "buffer"

        validatestr = self.validate_translation_columns(self.input_df, self.cols_list)
        try:
            if validatestr:
                print(validatestr)
            else:
                raise Exception("Failed columns validation (translation columns) {}".format(self.cols_list))
        except ValueError as e:
            raise Exception("Failed columns validation (translation columns) {}".format(self.cols_list))
        

        if os.path.isfile(self.output_file_path):
            try:
                self.merge_df = pd.read_csv(self.output_file_path, sep=self.delimiter, header=0)
            except (IOError,NameError,Exception) as e:
                raise Exception("Could not read the file) {0} {1} {2}".format(self.input_df_or_path_or_buffer,e,sys.stderr))

        self.analyze_the_inputs()
        if self.any_valid_for_translation():
            print("Starting translations")
            self.export_df = pd.DataFrame({self.id_variable : self.input_df[self.id_variable].tolist()})
            self.export_df = self.set_dtype_to_df_from_dict(self.export_df)
            for idx, col in enumerate(self.cols_list):
                translate_col = self.translate_to_cols[idx]
                if not (self.source_translated_dfs[col].empty and self.to_translate_dfs[col].empty):
                    self.translate_retry_loop(col, translate_col)
                    df = self.set_dtype_to_df_from_dict(pd.DataFrame(columns=[self.id_variable, col, translate_col]))
                    if not self.source_translated_dfs[col].empty:
                        df = df.append(self.source_translated_dfs[col], ignore_index=True, sort=False)
                    if not self.translated_dfs[col].empty:
                        df = df.append(self.translated_dfs[col], ignore_index=True, sort=False)
                    if not df.empty:
                        self.export_df = pd.merge(self.export_df, self.set_dtype_to_df_from_dict(df), how='left', on=self.id_variable, sort=True, copy=True)
                else:
                    self.export_df[col] = ""
                    self.export_df[translate_col] = ""
            self.export_df = self.set_dtype_to_df_from_dict(self.export_df)

            if hasattr(self,"error_response"):
                if self.error_response["response"] == "fatal":
                    print("Fatal error, outputting file with any translations done")
                    print("The process can be re run against the output file")

            if not os.path.exists(os.path.dirname(self.output_file_path)):
                os.mkdir(os.path.dirname(self.output_file_path))
            if os.path.exists(self.output_file_path):
                os.remove(self.output_file_path)

            self.export_df.to_csv(self.output_file_path, sep=self.delimiter, encoding='utf-8', index=False, quotechar='"',
                                header=True, quoting=csv.QUOTE_NONNUMERIC)
        print("Script Complete")
        return

    def analyze_the_inputs(self):
        """

        Check the input file and the output file (it will create a dummy 'merge' if an output file is not found)

        Workout what strings require transations for each column:
        - source_translated_dfs - contains strings already translated
        - to_translate_dfs - contains strings that need to be translated

        """
        print("Analyzing inputs")
        for col in self.translate_to_cols:
            if not col in self.input_df.columns:
                self.input_df[col] = ""
        self.all_cols = [self.id_variable] + list(self.cols_list) + list(self.translate_to_cols)
        if not hasattr(self,"merge_df"):
              self.merge_df = pd.DataFrame(columns=self.all_cols)
        else:
            for col in self.translate_to_cols:
                if not col in self.merge_df.columns:
                    self.merge_df[col] = ""

        self.input_df = self.set_dtype_to_df_from_dict(self.input_df)
        self.merge_df = self.set_dtype_to_df_from_dict(self.merge_df)
        self.estimate_batches()
        for idx, col in enumerate(self.cols_list):
            translate_col = self.translate_to_cols[idx]
            input_df = self.get_input_col_dfs("input", col, translate_col)
            merge_df = self.get_input_col_dfs("merge", col, translate_col)
            if not (input_df.empty and merge_df.empty): 
                df = pd.merge(input_df, merge_df, how='left', on=self.id_variable, sort=True, suffixes=('_input', '_merge'), copy=True)
                df[translate_col] = df["{}_merge".format(translate_col)].combine_first(df["{}_input".format(translate_col)])
                df = df.rename(columns={"{}_input".format(col): col})
                df = df[[self.id_variable, col, translate_col]]
                self.source_translated_dfs[col] = df.loc[(df[col].str.len() > 0) & (df[translate_col].str.len() > 0)].copy()
                self.to_translate_dfs[col] = df.loc[(df[col].str.len() > 0) & (df[translate_col].str.len() == 0)].copy()
                print("{0} > {1}: {2} translations found in output, {3} strings to translate".format(col,translate_col,len(self.source_translated_dfs[col]),len(self.to_translate_dfs[col])))
            else:
                print("{0} > {1}: No data to translate".format(col,translate_col))


    def translate_retry_loop(self, col, translate_col):
        """

        A while loop processes translations for a particular column (col) as many times as needed and will only finish when all strings are translated of if a fatal error is found.

        All translations are done when the len of 'to_translate_dfs[col]' is equal to 'translated_dfs[col]'

        """
        if self.is_valid_for_translation(col):
            print("Translating '{}'".format(col))
            while not self.col_translated(col) and self.error_response["response"] != "fatal":
                self.ms_translate_col(col, translate_col)
                if self.error_response["response"] == "fatal":
                    break

    
    def ms_translate_col(self, col, translate_col, **kwargs):
        """

        Process translations for a particular column (col) as many times as needed.
        
        As strings (or a batch of strings) are translated 'translated_dfs[col]' is populated.

        Translations are done in batches, MS allows for 25 strings per request with a maximum total string length of 5000 characters:
        - The optimum batch size (batchcounter) has been worked out in estimate_batches function based on the average string length in the column.
        - If a string length is less than (5000 / batch size) then it can be added to a batch without any concern.
        - If a string length is more than (5000 / batch size) the string could be too long, a number of strings like this could make the total string length more than 5000 characters.  These strings are not added to a batch, they are translated one at a time.

        If any error is found the 'resolve_retry_error' function is called.

        """

        if col in self.batch_translate_cols:
            batchcounter = self.batch_list_input[col]
            to_translate_df = self.to_translate_dfs[col].copy()
        elif col in self.dup_translate_cols:
            batchcounter = 25
            to_translate_df = self.to_translate_dfs[col].drop_duplicates(subset=[col]).copy()

        startbatchi = 0
        endbatchi = startbatchi + batchcounter
        df_len = len(to_translate_df)
        display_counter_batch = 0
        for i in range(0, int(round(df_len / batchcounter, 0)) + 1):
            display_counter_batch += 1
            endbatchi = self.get_endbatchi(startbatchi, endbatchi, batchcounter, df_len)
            if col in self.batch_translate_cols:
                text_batch_input = to_translate_df[col].iloc[startbatchi:endbatchi].tolist()
                serials_batch_input = to_translate_df[self.id_variable].iloc[startbatchi:endbatchi].tolist()
            elif col in self.dup_translate_cols:
                text_batch_input = to_translate_df[col].iloc[startbatchi:endbatchi].tolist()

            translated_batched = []
            translated_single_batch = []
            text_batched = []
            text_single_batch = []
            serials_batched = []
            serials_single_batch = []

            for idx, text in enumerate(text_batch_input):
                if not len(text) > int(5000 / batchcounter):
                    text_batched.append(text)
                    if col in self.batch_translate_cols:
                        serials_batched.append(serials_batch_input[idx])
                else:
                    trans_dict = self.ms_translate_single_text(text[0:4999], self.subscriptionKey, self.tolang, self.fromlang)
                    text_single_batch.append(text)
                    translated_single_batch.append(trans_dict["translation"])
                    if col in self.batch_translate_cols:
                        serials_single_batch.append(serials_batch_input[idx])

            json_request_body = []
            for text in text_batched:
                json_request_body.append({'text': '{}'.format(text)})

            try:
                response = requests.post(self.url, headers=self.get_ms_translate_headers(self.subscriptionKey), json=json_request_body)
            except(ConnectionResetError):
                print("Connection Error.\r\ntranslations:{}".format(json_request_body),1)
                self.resolve_retry_error(col, translate_col,"connection_error", response)
                return
            except(Exception):
                print("Error Sending the request to Microsoft.\r\ntranslations:{}".format(json_request_body),1)
                self.resolve_retry_error(col, translate_col,"response_exception_error", response)
                return
            try:
                jsontext = response.json()
            except(Exception):
                print("The response json could not be converted to a dictionary",1)
                self.resolve_retry_error(col, translate_col, "json_error",response)
                return 
            try:
                testjson = jsontext[0]['translations'][0]['text']
            except(Exception):
                print("The response json could not find a translation response",1)
                self.resolve_retry_error(col, translate_col, "find_translation_error", response)
                return

            for idx, translation_response in enumerate(jsontext):
                translated_batched.append(translation_response['translations'][0]['text'])

            if col in self.batch_translate_cols:
                try:
                    if serials_batched and text_batched and translated_batched:
                        length = len(serials_batched)
                        if all(len(serials_batched) == length for lst in [text_batched, translated_batched]):
                            self.translated_dfs[col] = self.translated_dfs[col].append(self.set_dtype_to_df_from_dict(pd.DataFrame({self.id_variable : serials_batched, col : text_batched, translate_col : translated_batched})), sort=False)
                        
                    if serials_single_batch and text_single_batch and translated_single_batch:
                        length = len(serials_single_batch)
                        if all(len(serials_single_batch) == length for lst in [text_single_batch, translated_single_batch]):
                            self.translated_dfs[col] = self.translated_dfs[col].append(self.set_dtype_to_df_from_dict(pd.DataFrame({self.id_variable : serials_single_batch, col : text_single_batch, translate_col : translated_single_batch})), sort=False)
                    
                    if self.retry_counter > 0:
                        print("A successfull translation has been done after a retry")
                        self.retry_counter = 0
                except:
                    print("Could not complile translated data.\rserials_batch:\r\n{0}\rtext_batch:\r\n{1}\rtranslated_batch:\r\n{2}".format(serials_batched,text_batched,translated_batched),1)
                    self.resolve_retry_error(col, translate_col, "translation_compile_error",  response)
                    return

            elif col in self.dup_translate_cols:
                if len(text_batched) == len(translated_batched):
                    for idx, text in enumerate(text_batched):
                        text_serials = self.to_translate_dfs[col][self.id_variable].loc[(self.to_translate_dfs[col][col] == text)].values.tolist()
                        self.translated_dfs[col] = self.translated_dfs[col].append(self.set_dtype_to_df_from_dict(pd.DataFrame({self.id_variable : text_serials, col : [text] * len(text_serials), translate_col : [translated_batched[idx]] * len(text_serials)})), sort=False)
                    
                    if self.retry_counter > 0:
                        print("A succesfull translation has been done after a retry")
                        self.retry_counter = 0
                else:
                    print("Could not complile translated data.\rserials_batch:\r\n{0}\rtext_batch:\r\n{1}\rtranslated_batch:\r\n{2}".format(serials_batched,text_batched,translated_batched),1)
                    self.resolve_retry_error(col, translate_col, "translation_compile_error",  response)
                    return

            if endbatchi >= df_len or self.col_translated(col):
                print("\t{} batches translated".format(display_counter_batch))
                return
            startbatchi = endbatchi
            if display_counter_batch % 10 == 0:
                print("\t{} batches translated".format(display_counter_batch))

    def resolve_retry_error(self, col, translate_col, type_error, response=None):
        
        self.error_response = {"col" : col, "translate_col" : translate_col, "type_error" : type_error, "response" : "fatal"}
 
        try:
            jsontext = response.json()
        except:
            jsontext = {}

        if jsontext:
            if "error" in jsontext:
                if "code" in jsontext["error"] and "message" in jsontext["error"]:
                    self.error_response["code"] = jsontext["error"]["code"]
                    self.error_response["message"] = jsontext["error"]["message"]
                    if self.error_response["code"] in [400000, 400077, 403001, 408001, 408002, 429000, 429001, 429002, 500000, 503000]:
                        self.error_response["response"] = "retry"

        if "code" in self.error_response and "message" in self.error_response:
            print("{0}: '{1}'".format(self.error_response["code"],self.error_response["message"]))

        if self.error_response["response"] =="retry":
            print("{}: Will wait and retry, resolving any translations already done".format(type_error))
            self.retry_counter +=1 
            if not self.translated_dfs[col].empty:
                self.source_translated_dfs[col] = self.source_translated_dfs[col].append(self.translated_dfs[col],ignore_index=True,sort=False)
                df = self.set_dtype_to_df_from_dict(pd.DataFrame({"Translated_ID" : self.source_translated_dfs[col][self.id_variable].values.tolist()}))
                translate_df = self.set_dtype_to_df_from_dict(pd.merge(self.to_translate_dfs[col], df, how='left', left_on=self.id_variable, right_on="Translated_ID", sort=True, copy=True))
                translate_df = translate_df.loc[(df["Translated_ID"].str.len() == 0)]
                self.to_translate_dfs[col] = translate_df[[self.id_variable,col, translate_col]].copy()
                self.translated_dfs[col] = self.translated_dfs[col].iloc[0:0]

            if not self.to_translate_dfs[col].empty and self.retry_counter > 0:
                if self.retry_counter < 5:
                    while True:
                        print("Delay for 1 minute",1)
                        time.sleep(60) # Delay for 1 minute (60 seconds).
                if self.retry_counter < 10:
                    while True:
                        print("Delay for 5 minutes",1)
                        time.sleep(300) # Delay for 5 minutes(300 seconds).
                if self.retry_counter <= 12:
                    while True:
                        print("Delay for 10 minutes",1)
                        time.sleep(600) # Delay for 10 minutes(600 seconds).
                if self.retry_counter > 12:
                    self.error_response["response"] = "fatal"

        if self.error_response["response"] =="fatal":
            print("{}: Fatal error found, will end the process, will save output file so that any translations will be saved".format(type_error), 2, raise_error_exception=False)


    def get_dtype_for_frame(self, df_cols):
        dtypes = {}
        for col in list(df_cols):
            if col in self.dtypes.keys():  
                dtypes[col] = self.dtypes[col]
        return dtypes

    def set_dtype_to_df_from_dict(self, df):
        dtypes = self.get_dtype_for_frame(list(df.columns))
        for col, dtype in dtypes.items():
            df[col] = df[col].apply(dtype)
            if dtype == str:
                df[col] = df[col].replace(["nan",np.nan,None],"")
                if not col == self.id_variable:
                    df[col] = df[col].apply(self.clean_str)
        return df

    def clean_str(self, input_value):
        if input_value:
            input_str = str(input_value)
            input_str = input_str.replace("\r\n"," ").replace("\n"," ").replace("\r"," ")
            if self.capitalize:
                input_str = input_str.capitalize()
            return input_str
        else:
            return ""


    def get_input_col_dfs(self, name, col, translate_col):
        
        if name == "input":
            out_df = self.input_df[[self.id_variable, col, translate_col]].copy()
            out_df = out_df[out_df[col] != ""]
        if name == "merge":
            out_df = self.merge_df[[self.id_variable, col, translate_col]].copy()
            out_df = out_df[out_df[translate_col] != ""]
        out_df = self.set_dtype_to_df_from_dict(out_df)
        
        return out_df


    def col_translated(self, col):
        if self.is_valid_for_translation(col):
            if len(self.translated_dfs[col]) == len(self.to_translate_dfs[col]):
                return True
        return False

    def all_cols_translated(self):
        counter = 0
        valid_counter = 0
        for col in self.cols_list:
            if self.is_valid_for_translation(col):
                valid_counter += 1
            if self.col_translated(col):
                counter += 1

        if counter == valid_counter:
            return True
        else:
            return False

    def any_valid_for_translation(self):
        valid_counter = 0
        for col in self.cols_list:
            if self.is_valid_for_translation(col):
                valid_counter += 1

        if valid_counter > 0:
            return True
        else:
            return False

    def is_valid_for_translation(self, col):
        if len(self.to_translate_dfs[col]) > 0:
            return True
        return False


    @staticmethod
    def validate_translation_columns(df, cols_list):
        """

        Validates the the columns selected for validation exist in the data frame.

        Returns string

        """
        try:
            for col in cols_list:
                testdf = df[col]
            return "All columns selected found in df"
        except (ValueError, KeyError) as e:
            print("Column cannot be found in df: {0}".format(e))
            return ""


    @staticmethod
    def ms_translate_single_text(text, subscriptionKey, tolang="en", fromlang=None):
        """

        Translates a single string (text) using the MS translation api..

        Returns dictionary of translation response.

        """
        url = Translate_App.get_constructed_url(tolang, fromlang)
        try:
            request = requests.post(url, 
                headers=Translate_App.get_ms_translate_headers(subscriptionKey), 
                json=[{'text': '{}'.format(text)}])
        except:
             print("Error Sending the request to Microsoft.\r\ntext: {}".format(text))
             return {}
        if "error" in request:
            if "code" in request["error"] and "message" in request["error"]:
                print("API Error Code {0}: '{1}'".format(request["error"]["code"],request["error"]["message"]))
            else:
                print("Unknown error")
            return {}
        try:
            jsontext = request.json()
        except:
            print("Error Sending the request to Microsoft.\r\ntext: {}".format(text))
            return {}

        info_dict = {}
        info_dict["text"] = text
        info_dict["translation"] = jsontext[0]['translations'][0]['text']
        if not "from=" in url:
            info_dict["detectedlang"] = jsontext[0]['detectedLanguage']['language']
            info_dict["detectedscore"] = jsontext[0]['detectedLanguage']['score']
        return info_dict

    @staticmethod
    def get_constructed_url(tolang, fromlang=None):
        url = "https://api.cognitive.microsofttranslator.com/translate?api-version=3.0"
        if fromlang:
            return "{0}&from={1}&to={2}".format(url, fromlang, tolang)
        else:
            return "{0}&to={1}".format(url, tolang)

    @staticmethod
    def get_ms_translate_headers(subscriptionKey):
        """

        Generates the headers required for the MS translation api.

        Returns dictionary.

        """
        headers = {
            'Ocp-Apim-Subscription-Key': subscriptionKey,
            'Content-type': 'application/json',
            'X-ClientTraceId': str(uuid.uuid4())
        }
        return headers


    def estimate_batches(self):
        """

        Works out the maximum batch size based on the longest string in a series.
        
        The MS translation api has a maximum batch is 25, max length is 5000 characters:

        Returns data frame trans_df

        """

        for col in self.cols_list:
            df = self.input_df[[self.id_variable,col]].loc[(self.input_df[col].str.len() > 0)]
            unique_texts = len(df[col].unique())
            if unique_texts / len(df) > 0.05:
                self.batch_translate_cols.append(col)
            else:
                self.dup_translate_cols.append(col)

        self.batch_list_input = {}
        for col in self.batch_translate_cols:
            df = self.input_df[[self.id_variable,col]].loc[(self.input_df[col].str.len() > 0)]
            df_col_mean = int(df[col].str.len().mean())

            if df_col_mean < 200:
                self.batch_list_input[col] = 25
            elif df_col_mean < 209:
                self.batch_list_input[col] = 24
            elif df_col_mean < 218:
                self.batch_list_input[col] = 23
            elif df_col_mean < 228:
                self.batch_list_input[col] = 22
            elif df_col_mean < 239:
                self.batch_list_input[col] = 21
            elif df_col_mean < 250:
                self.batch_list_input[col] = 20
            elif df_col_mean < 264:
                self.batch_list_input[col] = 19
            elif df_col_mean < 278:
                self.batch_list_input[col] = 18
            elif df_col_mean < 295:
                self.batch_list_input[col] = 17
            elif df_col_mean < 313:
                self.batch_list_input[col] = 16
            elif df_col_mean < 334:
                self.batch_list_input[col] = 15
            elif df_col_mean < 358:
                self.batch_list_input[col] = 14
            elif df_col_mean < 385:
                self.batch_list_input[col] = 13
            elif df_col_mean < 417:
                self.batch_list_input[col] = 12
            elif df_col_mean < 455:
                self.batch_list_input[col] = 11
            elif df_col_mean < 500:
                self.batch_list_input[col] = 10
            elif df_col_mean < 556:
                self.batch_list_input[col] = 9
            elif df_col_mean < 625:
                self.batch_list_input[col] = 8
            elif df_col_mean < 715:
                self.batch_list_input[col] = 7
            elif df_col_mean < 834:
                self.batch_list_input[col] = 6
            elif df_col_mean < 1000:
                self.batch_list_input[col] = 5
            elif df_col_mean < 1250:
                self.batch_list_input[col] = 4
            elif df_col_mean < 1667:
                self.batch_list_input[col] = 3
            elif df_col_mean < 2500:
                self.batch_list_input[col] = 2

    @staticmethod
    def get_endbatchi(startbatchi, endbatchi, batchcounter, todo_len):
        """

        Works out the end of a batch based on the start of a batch and the batch size (taking in account the total to do)

        Returns integer 

        """
        if endbatchi > todo_len:
                return todo_len
        else:
            return startbatchi + batchcounter

    @staticmethod
    def delimiter_validation(input_delimiter, output_desc=False, raise_error=True):
        desc_delimiters = ["tab", "comma", "semicolon", "pipe"]
        actual_delimiters = ["\t",",",";","|"]
        if input_delimiter in actual_delimiters:
            if not output_desc:
                return input_delimiter
            else:
                if input_delimiter == "\t":
                    return "tab" 
                elif input_delimiter == ",":
                    return "comma"
                elif input_delimiter == ";":
                    return "semicolon"
                elif input_delimiter == "|":
                    return "pipe"
        elif input_delimiter in desc_delimiters:
            if output_desc:
                return input_delimiter
            else:
                if input_delimiter == "tab":
                    return "\t"
                elif input_delimiter == "comma":
                    return ","
                elif input_delimiter == "semicolon":
                    return ";"
                elif input_delimiter == "pipe":
                    return "|"
        else:
            err = "The delimiter provided '{0}' is not valid, it must be one of {1} or {2}".format(input_delimiter, actual_delimiters, desc_delimiters)
            if raise_error:
                print(err)
            return None


def run_batch_translate(input_df_or_path_or_buffer, output_file_path, translate_cols_list, subscriptionKey, id_variable, tolang="en", fromlang=None, delimiter=",",translated_suffix="_Translated", dtypes=None, capitalize=True):
    """

    Batch Microsoft Translator allows for large amounts of translations using the Microsoft 'Translator Text' API reliably.

    Before starting a user must have a Microsoft Azure service account and must set up an account for the api to obtain a subscription key.

    More information can be found here: https://azure.microsoft.com/en-gb/services/cognitive-services/translator-text-api/

    - input_df_or_path_or_buffer (DataFrame/str/StringIO) - the input can come from 1 of 3 sources:
        - pandas DataFrame (DataFrame)
        - input file path (str)
        - a file buffer (StringIO)
    - output_file_path (str) - the path where the output will be exported, or read from if needed.
    - translate_cols_list (list) - the columns to translate in the data.
    - subscriptionKey (str) - the subscription key from your Microsoft Azure service account.
    - id_variable (str) - the process requires an id variable to exist in the data.
    - tolang (str) - the language to translate to (default is "en"). More langauge codes can be found on the microsoft website.
    - fromlang (str) - the language to translate to (default is None). If None the process will detect the language.
    - delimiter (str) - the delimiter to use for teh input and output files (default is ",").
    - translated_suffix (str) - the suffix to apply to the name of the translation columns (default is "_Translated").
    - dtypes (str) - any dtypes to apply when loading a input file (note all translation columns will be converted to str) 
    - capitalize (bool) - with clean text to 'proper' case if True (default is True)

    """
    input_dict = {"input_df_or_path_or_buffer" : input_df_or_path_or_buffer,
            "output_file_path" : output_file_path,
            "translate_cols_input" : ";".join(translate_cols_list),
            "subscriptionKey" : subscriptionKey,
            "tolang" : tolang,
            "fromlang" : fromlang,
            "delimiter" : delimiter,
            "id_variable" : id_variable,
            "translated_suffix" : translated_suffix,
            "dtypes" : dict(dtypes),
            "capitalize" : capitalize}

    config = Translate_App(input_dict)
    if config.ERROR:
        print("Translate_App: Errors Found in the validation process, the app will not start")
        for err in config.ERROR:
            print(err, 2, False)
        
    else:
        config.translate_app_main()
        return