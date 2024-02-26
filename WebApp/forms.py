from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField
from wtforms.validators import DataRequired, Length, Email, EqualTo, NumberRange, ValidationError


class SearchnForm(FlaskForm):
    query = StringField('Search Query', validators=[DataRequired(), Length(min=1, max=100)])
    search = SubmitField('Search')

class TfidfForm(FlaskForm):
    start_id = IntegerField('Document Start ID', validators=[DataRequired(), NumberRange(min=1, max=68502)], default = 1)
    end_id = IntegerField('Document End ID', validators=[DataRequired(), NumberRange(min=1, max=68502)], default = 5)
    n_mappers = IntegerField('Number of Mappers', validators=[DataRequired(), NumberRange(min=1, max=10)], default = 3)
    n_reducers = IntegerField('Number of Reducers', validators=[DataRequired(), NumberRange(min=1, max=10)], default = 3)
    calculate = SubmitField('Calculate')
        
    