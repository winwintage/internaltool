from flask import Flask, request, jsonify, make_response, send_file, render_template
import mysql.connector
from dotenv import load_dotenv
import os
import csv
from mysql.connector.constants import ClientFlag
from constants import getQuery, table_list, getSelectQuery, uploadCSVFormatMap
import datetime
import io
from flask_cors import CORS, cross_origin

load_dotenv()

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# Create a connection with SQL instance
connection = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_DATABASE"),
    user=os.getenv("DB_USER"),
    client_flags=[ClientFlag.SSL],
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
    ssl_ca='ca-certificate.crt'
)


def make_sure_connection_is_present():
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
    except mysql.connector.errors.OperationalError as e:
        # The connection has been closed or has timed out, so we need to reconnect
        print("Connection is not present, trying to reconnect with error:", e.msg)
        connection.reconnect()


def insert_data(cur, reader, table_name):
    rows = []
    for i, row in enumerate(reader):
        try:
            query = getQuery(table_name, row)
            cur.execute(query)
            rows.append((i+1, row, "SUCCESS", ""))
        except Exception as e:
            rows.append((i+1, row, "FAILED", str(e)))
    return rows


@app.route('/', methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')


# Upload CSV route
@app.route('/uploadCsv', methods=['POST'])
@cross_origin()
def uploadCsv():
    try:
        if not request.files['file']:
            return jsonify({'error': "No file found"}), 400

        uploaded_file = request.files['file']
        table_name = request.form['table']

        if table_name not in table_list:
            return jsonify({'error': "Table not present in DB"}), 400

        excepted_csv_headers = uploadCSVFormatMap[table_name]

        make_sure_connection_is_present()
        cur = connection.cursor()

        file_data = uploaded_file.read().decode("utf-8")
        file_reader = csv.DictReader(file_data.splitlines())

        header = file_reader.fieldnames
        if sorted(header) != sorted(excepted_csv_headers):
            return jsonify({'error': "Header in the uploaded CSV does not match the expected CSV headers. Cross check the CSV header values given on previous page and retry Provided Values: {}, Expected Headers: {}".format(header, excepted_csv_headers)}), 400

        failed_rows = insert_data(cur, file_reader, table_name)

        connection.commit()
        cur.close()
        stringIO = io.StringIO()
        writer = csv.writer(stringIO)

        if failed_rows:
            failed_csv_name = "{}_failed_rows_{}.csv".format(
                table_name, datetime.datetime.now())

            writer.writerow(["Row Number", "Row Status",
                            "Error Message", *header])
            for i, row, rowStatus, error in failed_rows:
                writer.writerow([i, rowStatus, error, *list(row.values())])

            output = make_response(stringIO.getvalue())
            output.headers["Content-Disposition"] = "attachment; filename={}".format(
                failed_csv_name)
            output.headers["Content-type"] = "text/csv"
            return output
        else:
            return jsonify({'message': 'All rows inserted successfully.'}), 201
    except Exception as e:
        return jsonify({'error': 'Failed to process. Reason: {}'.format(str(e))}), 500


@app.route('/getData', methods=['POST'])
@cross_origin()
def getData():
    if not request.form['table_name']:
        return jsonify({'error': "Table name is mandatory"}), 400

    table_name = request.form['table_name']

    if table_name not in table_list:
        return jsonify({'error': "Table not present in DB"}), 400

    filename = "{}.csv".format(table_name)
    try:
        query = getSelectQuery(table_name)

        make_sure_connection_is_present()
        cur = connection.cursor()
        cur.execute(query)
        data = cur.fetchall()

        # Get the column names
        headers = [i[0] for i in cur.description]

        # Save the data with headers to a CSV file
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(data)

        cur.close()

        # Return the CSV file as a download
        return send_file(filename,
                         mimetype='text/csv',
                         as_attachment=True)
    except Exception as e:
        return jsonify({'error': 'Failed to process. Reason: {}'.format(str(e))}), 500
    finally:
        # Clean up the file after the request is processed
        if os.path.exists(filename):
            os.remove(filename)


# Driver function
if __name__ == '__main__':
    app.run(debug=True)
