# coding: utf-8
from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, Index, Integer, String, Table
from sqlalchemy.schema import FetchedValue
from sqlalchemy.orm import relationship
from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


class Biosample(db.Model):
    __tablename__ = 'biosample'

    biosample_id = db.Column(db.Integer, primary_key=True, server_default=db.FetchedValue())
    donor_id = db.Column(db.ForeignKey('donor.donor_id', ondelete='CASCADE', onupdate='RESTRICT'), nullable=False)
    source_id = db.Column(db.String, nullable=False, unique=True)
    type = db.Column(db.String)
    tissue = db.Column(db.String)
    tissue_tid = db.Column(db.Integer)
    cell_line = db.Column(db.String)
    cell_line_tid = db.Column(db.Integer)
    is_healthy = db.Column(db.Boolean)
    disease = db.Column(db.String)
    disease_tid = db.Column(db.Integer)

    donor = db.relationship('Donor', primaryjoin='Biosample.donor_id == Donor.donor_id', backref='biosamples')


t_case2item = db.Table(
    'case2item',
    db.Column('item_id', db.ForeignKey('item.item_id', ondelete='CASCADE', onupdate='RESTRICT'), primary_key=True, nullable=False),
    db.Column('case_id', db.ForeignKey('case_study.case_study_id', ondelete='CASCADE', onupdate='RESTRICT'), primary_key=True, nullable=False)
)


class CaseStudy(db.Model):
    __tablename__ = 'case_study'

    case_study_id = db.Column(db.Integer, primary_key=True, server_default=db.FetchedValue())
    project_id = db.Column(db.ForeignKey('project.project_id', ondelete='CASCADE', onupdate='RESTRICT'), nullable=False)
    source_id = db.Column(db.String, nullable=False, unique=True)
    source_site = db.Column(db.String)
    external_ref = db.Column(db.String)

    project = db.relationship('Project', primaryjoin='CaseStudy.project_id == Project.project_id', backref='case_studies')
    items = db.relationship('Item', secondary='case2item', backref='case_studies')


class Dataset(db.Model):
    __tablename__ = 'dataset'

    dataset_id = db.Column(db.Integer, primary_key=True, server_default=db.FetchedValue())
    name = db.Column(db.String, nullable=False, unique=True)
    data_type = db.Column(db.String)
    format = db.Column(db.String)
    assembly = db.Column(db.String)
    is_ann = db.Column(db.Boolean)
    annotation = db.Column(db.String)
    annotation_tid = db.Column(db.Integer)


class Donor(db.Model):
    __tablename__ = 'donor'

    donor_id = db.Column(db.Integer, primary_key=True, server_default=db.FetchedValue())
    source_id = db.Column(db.String, nullable=False, unique=True)
    species = db.Column(db.String)
    species_tid = db.Column(db.Integer)
    age = db.Column(db.Integer)
    gender = db.Column(db.String)
    ethnicity = db.Column(db.String)
    ethnicity_tid = db.Column(db.Integer)


class ExperimentType(db.Model):
    __tablename__ = 'experiment_type'
    __table_args__ = (
        db.Index('technique_feature_target', 'technique', 'feature', 'target'),
    )

    experiment_type_id = db.Column(db.Integer, primary_key=True, server_default=db.FetchedValue())
    technique = db.Column(db.String)
    technique_tid = db.Column(db.Integer)
    feature = db.Column(db.String)
    feature_tid = db.Column(db.Integer)
    target = db.Column(db.String)
    target_tid = db.Column(db.Integer)
    antibody = db.Column(db.String)


class Item(db.Model):
    __tablename__ = 'item'

    item_id = db.Column(db.Integer, primary_key=True, server_default=db.FetchedValue())
    experiment_type_id = db.Column(db.ForeignKey('experiment_type.experiment_type_id', ondelete='CASCADE', onupdate='RESTRICT'), nullable=False)
    dataset_id = db.Column(db.ForeignKey('dataset.dataset_id', ondelete='CASCADE', onupdate='RESTRICT'), nullable=False)
    source_id = db.Column(db.String, nullable=False, unique=True)
    size = db.Column(db.BigInteger)
    date = db.Column(db.String)
    checksum = db.Column(db.String)
    platform = db.Column(db.String)
    platform_tid = db.Column(db.Integer)
    pipeline = db.Column(db.String)
    source_url = db.Column(db.String)
    local_url = db.Column(db.String)

    dataset = db.relationship('Dataset', primaryjoin='Item.dataset_id == Dataset.dataset_id', backref='items')
    experiment_type = db.relationship('ExperimentType', primaryjoin='Item.experiment_type_id == ExperimentType.experiment_type_id', backref='items')
    replicates = db.relationship('Replicate', secondary='replicate2item', backref='items')


class Project(db.Model):
    __tablename__ = 'project'

    project_id = db.Column(db.Integer, primary_key=True, server_default=db.FetchedValue())
    program_name = db.Column(db.String)
    project_name = db.Column(db.String, nullable=False, unique=True)


class Replicate(db.Model):
    __tablename__ = 'replicate'

    replicate_id = db.Column(db.Integer, primary_key=True, server_default=db.FetchedValue())
    biosample_id = db.Column(db.ForeignKey('biosample.biosample_id', ondelete='CASCADE', onupdate='RESTRICT'), nullable=False)
    source_id = db.Column(db.String, nullable=False, unique=True)
    bio_replicate_num = db.Column(db.Integer)
    tech_replicate_num = db.Column(db.Integer)

    biosample = db.relationship('Biosample', primaryjoin='Replicate.biosample_id == Biosample.biosample_id', backref='replicates')


t_replicate2item = db.Table(
    'replicate2item',
    db.Column('item_id', db.ForeignKey('item.item_id', ondelete='CASCADE', onupdate='RESTRICT'), primary_key=True, nullable=False),
    db.Column('replicate_id', db.ForeignKey('replicate.replicate_id', ondelete='CASCADE', onupdate='RESTRICT'), primary_key=True, nullable=False)
)
