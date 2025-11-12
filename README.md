# NBA Performance Prediction Model
## Overview
A machine learning model that uses a XGBoost Regressor to predict NBA player scoring performances.

## Purpose
As an avid basketball watcher, I’ve always been fascinated by how players can explode for 30/40/50-point games. Sure. Luka Dončić once went for a career-high 73 points, but why? Was it because the undersized Hawks played poor defense? Or did he have extra motivation after being traded on draft day for Trae Young?

Curious to get to the bottom of it, I searched for stats to help predict how many points a player might score. Over time, I developed a set of features that formed a matrix I could feed into a model; one capable of generating surprisingly accurate point predictions.

## Example Output
<img width="1388" height="621" alt="nba-proj-model" src="https://github.com/user-attachments/assets/94581ec9-1190-4eb3-ab19-2c2d3f38796b" />

## System Design
### `scrapers-[xxxxxx]`
Handles data scraping and cleaning before storing results in a MySQL database.

### `train_model.py`
Trains the model, performs backtesting, and exports the model using pickle.

### `test_model.py`
Loads the pickled model, feeds it production data, compares predictions against DraftKings lines for reference, and exports results to a CSV file.

## Data Sources that I used
- NBA API: Player and team statistics, box scores, usage rates, shot charts, and playstyle data
- DraftKings: Betting props
- ESPN: Injury reports
