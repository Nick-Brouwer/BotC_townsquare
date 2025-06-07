import nbtlib
# Load the scoreboard NBT file (gzip-compressed).
nbt = nbtlib.load('D:/Users/Nick/Downloads/scoreboard.dat')  
scores = nbt['data']['PlayerScores']  # list of all score entries

players = []
for entry in scores:
    if entry['Objective'] == 'Player':
        name = entry['Name']
        seat = abs(entry['Score'])
        players.append((name, seat))

players.sort(key=lambda x: x[1])

with open('player_seats.csv', 'w') as f:
    for name, seat in players:
        f.write(f"{name} {seat},")
 
 
    # for entry in scores:
    #     if entry['Objective'] == 'Player':
    #         name = entry['Name']
    #         seat = abs(entry['Score'])
    #         f.write(f"{name} {seat}\n")