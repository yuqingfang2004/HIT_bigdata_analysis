with open("../facebook/414.circles", "r") as infile, open("../csv_data/circles.csv", "w") as outfile:
    outfile.write("circle_name,member_id\n")
    for line in infile:
        parts = line.strip().split()
        if len(parts) > 1:
            circle_name = parts[0]
            members = parts[1:]
            for member in members:
                outfile.write(f"{circle_name},{member}\n")