# Batch Microsoft Translator
Batch Microsoft Translator allows for large amounts of translations using the Microsoft 'Translator Text' API reliably.
Before starting a user must have a Microsoft Azure service account and must set up an account for the api to obtain a subscription key.
More information can be found here: https://azure.microsoft.com/en-gb/services/cognitive-services/translator-text-api/
# Reliability
If any fatal errors are found the translations that have been done are written to the output file.  When re run the process with check the output file and start from where it left off. 
If any non fatal errors are found (e.g. internet connection issues) the process will wait and retry an number of times before raising a fatal error.

# Process Types
The Microsoft 'Translator Text' API only allows for a mixumum of 25 strings of a with a maximum total length of 5000 characters.
The process will analyze each column in the data to find the best way to translate in order to reduce cost.  There are 2 processes that a column can be translated by:
1. Batch Translations - Will go through each row and translate strings in batches, the process will work out the optimum batch size based on the average length of the strings found in each column.
2. Unique Translations - If the columns contains a large number of repeated strings the process will not go through each row. This process will find all unique strings and only translate those then apply them back to each row.

# Parameters
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
- dtypes (dict) - any dtypes to apply when loading a input file (note all translation columns will be converted to str) 
- capitalize (bool) - with clean text to 'proper' case if True (default is True)

# Example
~~~~python
from batch_microsoft_translator import run_batch_translate

input_file = "full_path"
output_file = "full_path"

run_batch_translate(input_file, output_file, ["Col1,Col2,Col3"], "XXXXXXXXXXXXXXXXXXXXXXXXXX", "ID", dtypes={"ID":int})
~~~~