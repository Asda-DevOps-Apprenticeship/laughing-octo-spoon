import os
from flask import Flask, request, render_template, flash, redirect, url_for
from app.flask_config import Config 
from app.models import db
from flask_migrate import Migrate
from app.utils import * 
from flask_apscheduler import APScheduler
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = APScheduler()


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config())
    
    scheduler.init_app(app)
    scheduler.start()
    
    app_data = {
    "name": "Adobe CDP GDPR Deletion & Deployment Tool",
    "description": "A basic Flask app using bootstrap for layout",
    "author": "Farai Wande",
    "html_title": "Peter's Starter Template for a Flask Web App",
    "project_name": "Adobe CDP GDPR Deletion & Deployment Tool",
    "keywords": "flask, webapp, template, basic",
}
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL','sqlite:///db.sqlite3')

    @app.route('/', methods=['GET'])
    def index():
        
        total_count, spids_by_date = get_spids_count_by_gdprdate()     
        return render_template("index.html", total_count=total_count, spids_by_date=spids_by_date, app_data=app_data)
    
    @app.route('/execute_deletions', methods=['POST'])
    def execute_deletions():
        try:
       
            deletion_date_str = request.form.get('deletion_date')
            if not deletion_date_str:
                flash("Please select a date.", "warning")
                return redirect(url_for('index'))

            deletion_date = datetime.strptime(deletion_date_str, '%Y-%m-%d').date()
            execute_gdpr_deletions_cdp(deletion_date, flash=flash)
        except Exception as e:
            logger.error(f"Error executing deletions: {e}")
            flash(f"An error occurred while executing deletions: {str(e)}", "danger")
        return redirect(url_for('index'))
                

    db.init_app(app)
    migrate = Migrate(app, db)
    return app


app = create_app()   
@scheduler.task('cron', id='daily_gdpr_run', day='*', hour=1, minute=2, misfire_grace_time=900)
def scheduled_task():
    
    logger.info("Starting scheduled GDPR deletions task")
    with app.app_context():
        try:
            delete_date = date.today()
            auto_execute_gdpr_deletions_cdp(delete_date)
            logger.info("Scheduled GDPR deletions task completed")
        except Exception as e:
            logger.error(f"Error executing scheduled deletions: {e}")
