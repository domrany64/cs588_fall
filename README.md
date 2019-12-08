# cs588_fall
This code is for the project of cloud DB course.
The code wil be run on a virtual machine on GCP which is one of the nodes from Cassandra cluster.
The code addresses 6 questions for the freeway data set.[^1]

The output of the code is similar to below lines:
```
Calculating the answer of question 1
The number of speed violations which are above 100 mph: 18
___________________________________________________________________________________________________________________________
Calculating the answer of question 2
The total volume for the station "FOSTER NB" on the mentioned date: 86563
___________________________________________________________________________________________________________________________
Calculating the answer of question 3
Showing sample #1 of 288 intervals: 2011-09-22 03:25:00 to 2011-09-22 03:30:00: 91.42857279096332
Showing sample #2 of 288 intervals: 2011-09-22 11:55:00 to 2011-09-22 12:00:00: 101.05263308474892
Showing sample #3 of 288 intervals: 2011-09-22 00:20:00 to 2011-09-22 00:25:00: 97.62712009882523
Showing sample #4 of 288 intervals: 2011-09-22 17:45:00 to 2011-09-22 17:50:00: 102.85714438983373
Showing sample #5 of 288 intervals: 2011-09-22 09:30:00 to 2011-09-22 09:35:00: 99.31034630742568
Showing sample #6 of 288 intervals: 2011-09-22 07:00:00 to 2011-09-22 07:05:00: 102.85714438983373
Showing sample #7 of 288 intervals: 2011-09-22 16:50:00 to 2011-09-22 16:55:00: 110.76923241982092
___________________________________________________________________________________________________________________________
Calculating the answer of question 4
The average speed: 41.5
Average travel time in seconds: 138.79518279110093
___________________________________________________________________________________________________________________________
Calculating the answer of question 5
Average travel time in minutes: 7.894592297548018
___________________________________________________________________________________________________________________________
Calculating the answer of question 6
The path is:
Johnson Cr NB -> Foster NB -> Powell to I-205 NB -> Division NB -> Glisan to I-205 NB -> Columbia to I-205 NB
___________________________________________________________________________________________________________________________
```

note that: 
[^1]The answer to q1 is not what it shoud be, the reason is that python times out when trying to connect to cassandra and run the query on a huge data set; hence, I ran this code on a smaller data set. The real answer is 6972!
2-The answer to q3 is partial since the entire number of them is 288 lines!
