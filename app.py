from flask import Flask
from flask import Blueprint, render_template, request, flash, jsonify, redirect, url_for, current_app
import plotly
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
app = Flask(__name__)

@app.route('/')
def hello_world():
    return render_template('public-dashboard.html',)
