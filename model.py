import pandas as pd
import seaborn as sns

sns.set(rc={'figure.figsize':(30,1)})

def visualise_sentiments(data):
  sns.heatmap(pd.DataFrame(data).set_index("song").T,center=0, annot=True, cmap = "PiYG")

song = """It might seem crazy what I'm 'bout to say
Sunshine she's here, you can take a break
I'm a hot air balloon that could go to space
With the air, like I don't care, baby, by the way

[Chorus]
(Because I'm happy)
Clap along if you feel like a room without a roof
(Because I'm happy)
Clap along if you feel like happiness is the truth
(Because I'm happy)
Clap along if you know what happiness is to you
(Because I'm happy)
Clap along if you feel like that's what you wanna do

[Verse 2]
Here come bad news, talking this and that (Yeah!)
Well, give me all you got, don't hold it back (Yeah!)
Well, I should probably warn ya, I'll be just fine (Yeah!)
No offense to you, don’t waste your time, here's why

[Chorus]
(Because I'm happy)
Clap along if you feel like a room without a roof
(Because I'm happy)
Clap along if you feel like happiness is the truth
(Because I'm happy)
Clap along if you know what happiness is to you
(Because I'm happy)
Clap along if you feel like that's what you wanna do

[Bridge]
Bring me down
Can't nothing bring me down
My level's too high to bring me down
Can't nothing bring me down, I said
Bring me down
Can't nothing bring me down
My level's too high to bring me down
Can't nothing bring me down, I said

[Chorus]
(Because I'm happy)
Clap along if you feel like a room without a roof
(Because I'm happy)
Clap along if you feel like happiness is the truth
(Because I'm happy)
Clap along if you know what happiness is to you
(Because I'm happy)
Clap along if you feel like that's what you wanna do
(Because I'm happy)
Clap along if you feel like a room without a roof
(Because I'm happy)
Clap along if you feel like happiness is the truth
(Because I'm happy)
Clap along if you know what happiness is to you
(Because I'm happy)
Clap along if you feel like that's what you wanna do

[Bridge]
Bring me down
Can't nothing bring me down
My level's too high to bring me down
Can't nothing bring me down, I said...

[Chorus]
(Because I'm happy)
Clap along if you feel like a room without a roof
(Because I'm happy)
Clap along if you feel like happiness is the truth
(Because I'm happy)
Clap along if you know what happiness is to you
(Because I'm happy)
Clap along if you feel like that's what you wanna do
(Because I'm happy)
Clap along if you feel like a room without a roof
(Because I'm happy)
Clap along if you feel like happiness is the truth
(Because I'm happy)
Clap along if you know what happiness is to you
(Because I'm happy)
Clap along if you feel like that's what you wanna do (Come on"""

# Import the NLTK model
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()

# Scores
sid.polarity_scores(song)

# Visualize
visualise_sentiments({
    "song":["song"] + song.split(),
    "Sentiment":[sid.polarity_scores(song)["compound"]] + [sid.polarity_scores(word)["compound"] for word in song.split()]
})