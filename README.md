## About this project. 

It is built based on spark extending package spark-jobserver(including the backend end service structure), I replace the spray framework by play, add the front end which is based on angularjs.

On the process, I used play route and angularjs route with html5mode, to combine routes together. It is implemented by the play demo project https://github.com/lashford/modern-web-template, which I could not understand how it is implemented. Because I am fresh to the front end and just spend several hours completing reading \<\<Professional JavaScript for Web Developers\>\>.(Of course, I bought(payed) the(for) book(it)...).

It solved a big question along a long time with my head. How to combine the front-end with back-end. Separately or how to together. Separate the front-end and back-end, which is really popular and I found many front-end developers are devoting this kind of style, is spread out the world with Nodejs. However, most of the time, I do not want to use two frameworks. I use the backend route for supporting static html pages. At the same time, I really want to enhance the ability of front end, so it could route some simple pages just in the client side. It is the best situation, right? 

Now I am still have one twirl template file in my project. I would remove it once I could find more about Play route... I really hate template in web design. It mixes html code with server code, which is the same as PHP. So here comes the question: PHP is the best language in the world. HAHAHAHA. How to defeat some guys not admit their mis-knowledge.

So just leave some words about the web framework...meaning less...

## How to run the project

git clone and sbt run......then view localhost:9000 ( so low style, I swear I would add docker later ....)

the difference between spark-jobserver and this project.

Tu Cao::

I do not think it is a good idea that run this project by submit it to spark cluster, which aims to avoid assembling the spark dependency into jar, and I guess as a surprising trick... However, it is up to the authors and I would like to keep the system standalone. Besides, the project folders structure is so terrible. Are you sure you want it or just show your sbt ability ( I am a sbt fresher...)

## How to Operate it.

I will write this part after completing algolibs and some demos.


personal todo list:

1. complete the algolibs project
    1. define the config format
    2. complete a extraction process
    3. complete the etl process demo
2. do a performance between hive and spark sql
3. complete the paper....