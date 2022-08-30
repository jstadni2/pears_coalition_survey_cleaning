:: Build the Docker image for pears_coalition_survey_cleaning.py
docker build -t il_fcs/pears_coalition_survey_cleaning:latest .
:: Create and start the Docker container
docker run --name pears_coalition_survey_cleaning il_fcs/pears_coalition_survey_cleaning:latest
:: Copy /example_outputs from the container to the build context
docker cp pears_coalition_survey_cleaning:/pears_coalition_survey_cleaning/example_outputs/ ./
:: Remove the container
docker rm pears_coalition_survey_cleaning
pause