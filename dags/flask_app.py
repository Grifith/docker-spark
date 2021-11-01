# flask_web/app.py

from flask import Flask, request, render_template
from etl_scripts.generic_functions import get_postgres_con

app = Flask(__name__)

@app.route('/')
def my_form():
    return render_template('my-form.html')

@app.route('/', methods=['POST'])
def my_form_post():
    text = request.form['text']
    processed_text = text.upper()
    con=get_postgres_con()
    cur=con.cursor()
    sql=" SELECT SUM(number_of_pages) FROM authors_pages WHERE UPPER(author)='{}'".format(processed_text)
    print("Executing.....{}".format(sql))
    cur.execute(sql)
    val=cur.fetchone()[0]
    if val:
        val=str(val)
    else:
        return ("Author Not Found")
    con.close()
    return ("total number of pages written by {} : {} ".format(processed_text,val))

if __name__ == '__main__':
	app.run(debug=True, host='0.0.0.0')
