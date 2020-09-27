- list 
- explode
- reset_index

- 6) Save the dataframes into the files :
* `appstore_games.normalized.csv` (shape : (16846, 11))
* `appstore_games_genres.normalized.csv` (shape : (44351, 3))
* `appstore_games_languages.normalized.csv` (shape : (54823, 2))



```bash
>> print(df_languages.head())
          ID Language
0  284921427       DA
1  284921427       NL
2  284921427       EN
3  284921427       FI
4  284921427       FR
```

```bash
>> print(df_genres.head())
          ID Primary Genre     Genre
0  284921427         Games  Strategy
1  284921427         Games    Puzzle
2  284926400         Games  Strategy
3  284926400         Games     Board
4  284946595         Games     Board
```