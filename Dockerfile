FROM python:3.9

WORKDIR /pears_coalition_survey_cleaning

COPY . .

RUN pip install -r requirements.txt

CMD [ "python", "./pears_coalition_survey_cleaning.py" ]