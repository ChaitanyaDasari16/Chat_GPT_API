from flask import Flask, render_template, request, send_file
import pandas as pd
import io
import json

from joblib.parallel import method
from openai import OpenAI

# Set up OpenAI API key
client = OpenAI(api_key='api-key')

def clean(dict_variable):
    return next(iter(dict_variable.values()))

app = Flask(__name__)

def corrupted_data_check(df, new_df):
    prompt = f"""Given this orginal refrence data:{df.to_dict('records')},
                and this is the new file : {new_df.to_dict('records')}, compare this new file with reference data and
                identify any column is corrupted or any data is wrongly populated or likely corrupted. Both the files may not be exactly 
                same but the data should be comaparitively close.
                return column_corrupted(column name) and reason in json format.
                Output in JSON form
            """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    print(response)
    new_rows = pd.DataFrame(clean(json.loads(response.choices[0].message.content)))
    print(new_rows)
    return new_rows


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Check if a file was uploaded
        if 'file' not in request.files:
            return 'No file uploaded', 400

        file = request.files['file']
        new_file = request.files['new_file']

        # Check if the file has a name
        if file.filename == '':
            return 'No file selected', 400

        if file and file.filename.endswith('.csv'):
            df = pd.read_csv(file)
            new_df = pd.read_csv(new_file)
            df_augmented = corrupted_data_check(df, new_df)

            output = io.BytesIO()
            df_augmented.to_csv(output, index=False)
            output.seek(0)

            return send_file(
                output,
                as_attachment=True,
                download_name='augmented' + file.filename,
                mimetype='text/csv'
            )

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)