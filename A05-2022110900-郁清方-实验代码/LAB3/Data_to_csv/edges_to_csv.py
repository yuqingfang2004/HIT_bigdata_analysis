with open("../facebook/414.edges", "r") as infile, open("../csv_data/edges.csv", "w") as outfile:
    outfile.write("source,target\n")
    for line in infile:
        parts = line.strip().split()
        if len(parts) == 2:
            outfile.write(f"{parts[0]},{parts[1]}\n")