# Create a statistically representative sample of your dataset. 
- The margin of error of 5%
- Confidence Level of 95%
- population size (size of appstore_games table)

Write a python function `sample_size` with the following parameters :
- `population_size` 
- `confidence_level` : default value `0.95`
- `margin_error` : default value `0.05`
- `standard_deviation` : default value `0.5`


- Doc https://www.wikihow.com/Calculate-Sample-Size

sample\_size = \frac{\frac{zscore^2 \times std(1 - std)}{margin\_error^2}}{1 + \frac{zscore^2 \times std(1 - std)}{margin\_error^2 \times Population\_size}}
$$

The z_score depends on the confidence level following this table:

|Confidence_level|Z_score|
|---|---|
|0.80|1.28|
|0.85|1.44|
|0.90|1.65|
|0.95|1.96|
|0.99|2.58|



# Some business use case examples 

- Show the top 100 games Name with the best user rating.
- You must show the Name of developers involved in games released before 01/08/2008 included and updated after 01/01/2018 included.
- You must create a program using a function `get_battle_royale` that shows the name of the games with "battle royale" (case insensitive) in their description and with a URL that will redirect to `facebook.com`.
- Show the first 10 games that generated the most benefits.Benefits are calculated with the number of users who voted times the price of the game.
- You must write a query that filters games according to the number of languages they have, and then filter out the ones that have strictly less than 3 languages. Then you need to select the top 5 genres where those games appear.


## Instructions

You must create a program using the function `get_top_100`.

This function must show the top 100 games Name ordered by `Avg_user_rating` first then by `Name`.

The names of games not starting with a letter must be ignored. Then, you must show the first 100 games starting with letters.

**You must only use PostgreSQL for your queries !**


## Example

```python
>> get_top_100()
AFK Arena
APORIA
AbsoluteShell
Action Craft Mini Blockheads Match 3 Skins Survival Game
Adrift by Tack
Agadmator Chess Clock
Age Of Magic
Age of Giants: Tribal Warlords
Age of War Empires: Order Rise
Alicia Quatermain 2 (Platinum)
...
```




# Analyze the price distribution of games by plotting a histogram of the price distribution. 
Then, you can use matplotlib to create a histogram. Your histogram will have to :
- not show games with a price below 1.0
- have a bar plot with 3 euros interval
- have the xlabel `Price`
- have the ylabel `Frequency`
- have the title `Appstore games price`
-> save price.png 

- use numpy to find the mean and the standard deviation of your data set.
```bash
> python price.py
mean price :  15.04098
std price :  6.03456
```

