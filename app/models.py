from flask_sqlalchemy import SQLAlchemy
from uuid import uuid4

db = SQLAlchemy()

class CddCustLoyaltyAcct(db.Model):
    __tablename__ = 'cdd_cust_loyalty_acct'
    singl_profl_id = db.Column(db.String, primary_key=True, nullable=False)
    wallet_id = db.Column(db.String, nullable=True)
    query_execution_date = db.Column(db.Date, nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<CddCustLoyaltyAcct singl_profl_id={self.singl_profl_id}>"

class DeletionJob(db.Model):
    __tablename__ = 'deletion_jobs'
    id = db.Column(db.String, primary_key=True, default=lambda: str(uuid4()))
    user_key = db.Column(db.String(50), nullable=False)
    job_id = db.Column(db.String(50), nullable=False)
    request_id = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<DeletionJob job_id={self.job_id}>"

class UserDeletions(db.Model):
    __tablename__ = 'user_deletions'
    id = db.Column(db.String, primary_key=True, default=lambda: str(uuid4()))
    singl_profl_id = db.Column(db.String, nullable=False)
    wallet_id = db.Column(db.String, nullable=True)
    gdprdate = db.Column(db.Date, nullable=False)
    deletion_flag = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<UserDeletions singl_profl_id={self.singl_profl_id}, wallet_id={self.wallet_id}, gdprdate={self.gdprdate}>"

class ProfileExportSnapshot(db.Model):
    __tablename__ = 'profile_export_snapshot'
    id = db.Column(db.String, primary_key=True, default=lambda: str(uuid4()))
    singl_profl_id = db.Column(db.String, nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<ProfileExportSnapshot singl_profl_id={self.singl_profl_id}>"
