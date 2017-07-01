var fs = require('fs');
var secret = require('./secret')

var Twit = require('twit');

var T = new Twit(secret);
var italyBoundings = [ 9.53, 36.14, 17.13, 48.19 ];
var tweets = [];
var dataPath = '../data/topics.twitter.json';

fs.readFile(dataPath, (err, old_tweets) => {
    if (!err) {
        tweets = JSON.parse(old_tweets);
        console.log("...resuming");
    }
    startStreaming();
});

/**
 * connects to Twitter ws
 * and scrapes tweets based on their location
 */
function startStreaming() {
    var stream = T.stream('statuses/filter', { locations: italyBoundings });

    stream.on('tweet', function (tweet) {
        topics = [];
        lng = 0;
        lat = 0;

        if (tweet.entities.hashtags.length > 0 ) {
            topics.push(...tweet.entities.hashtags.map(item => {return {keyword: low(item.text), weight: 1}}));
        }

        if (tweet.entities.user_mentions.length > 0) {
            topics.push(...tweet.entities.user_mentions.map(item => {return {keyword: low(item.name), weight: 1}}));
        } 

        if ( tweet.entities.symbols.length > 0) {
            topics.push(...tweet.entities.symbols.map(item => {return {keyword: low(item.text), weight: 1}}));
        }

        // if the coordinates are present  
        // else if a place with a bounding clip is present
        if (tweet.coordinates) {
            lng = +tweet.coordinates.coordinates[0].toFixed(5);
            lat = +tweet.coordinates.coordinates[1].toFixed(5);
        } else if (tweet.place.bounding_box.coordinates) {
            box = tweet.place.bounding_box.coordinates[0];
            bottomright = box[0];
            topleft = box[2];
            placeName = low(tweet.place.name);
            state = low(tweet.place.country);

            // if the bounding clips are distant less than 100 km on x axis
            // or the name of the place is not the state itself
            if ( (topleft[0] - bottomright[0] <= 1) || placeName !== state) {
                //console.log("ok")
                lng = +((topleft[0] + bottomright[0]) / 2).toFixed(5);
                lat = +((topleft[1] + bottomright[1]) / 2).toFixed(5);
            }
        } else {
            console.log(tweet);
        }

        if (topics.length > 0 && lng) {
            topicsList = topics.map(item => item.keyword);
            console.log(`${lat}, ${lng}: ${topicsList}`);
            tweets.push({lat, lng, topics});
        }
    })
}


/**
 * wait for CTRL-C
 * then save
 */
process.on('SIGINT', function() {
    if (tweets.length > 0) {
        console.log('Saving file, pls wait');
        fs.writeFile(dataPath, JSON.stringify(tweets), (err) => {
            if (err) {
                exit(err);
            } else {
                exit('Ok');
            }
        });
    } else {
        exit('No data collected, give more time next time');
    }  
});

function exit(outcome) {
    console.log(outcome);
    process.exit();
}

function low(string) {
    return string.toLowerCase();
}

// T.get('search/tweets', { geocode: '41.88,12.492,50km', count: 10 }, function(err, data, response) {
//   console.log(err)
//   console.log(data.statuses[0].user)
// })