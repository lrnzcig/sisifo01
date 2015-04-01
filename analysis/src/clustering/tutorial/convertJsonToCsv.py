'''
Created on 20/3/2015

@from: https://github.com/VikParuchuri/political-positions/make_matrix.py
'''
import unittest
import json

def convert_file(filename, out_filename):
    CONGRESS_NUM = "114"
    with open(filename) as f:
        senate = json.load(f)
    
    votes = [v for v in senate if v["congress"] == CONGRESS_NUM]
    
    senators = {}
    bills = []
    senator_names = []
    for v in votes:
        number = v["number"]
        for k, v in v["data"].items():
            if k not in senators:
                senators[k] = {}
            senators[k][number] = v
            bills.append(number)
            senator_names.append(k)
    
    bills = sorted(list(set(bills)))
    senator_names = sorted(list(set(senator_names)))
    
    vote_matrix = [["Name", "Party", "State"] + bills]
    for s in senator_names:
        data = s.replace(", ", "")
        name, info = data.split(" ")
        info = info.replace("(", "")
        info = info.replace(")", "")
        party, state = info.split("-")
        row = [name, party, state]
        for b in bills:
            vote = senators[s][b]
            code = "2"
            if vote == "Yea":
                code = "1"
            elif vote == "Nay":
                code = "0"
            row.append(code)
        vote_matrix.append(row)
    
    rows = [",".join(v) for v in vote_matrix]
    write_data = "\n".join(rows)
    with open(out_filename.format(CONGRESS_NUM), "w+") as f:
        f.write(write_data)
    return



class Test(unittest.TestCase):


    def testConvert(self):
        convert_file("/Users/lorenzorubio/git/political-positions/senate.json", "/Users/lorenzorubio/git/political-positions/senate.csv")
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testConvert']
    unittest.main()