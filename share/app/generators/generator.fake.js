var fs = require('fs');
var keywords = ['Matteo Renzi', 'Atalanta Bergamasca Calcio', 'Nintendo Switch', 'Luciano Spalletti', 'Matteo Salvini', 'Associazione Sportiva Roma', 'Paesi Bassi', 'Palermo', 'Mark Zuckerberg', 'WhatsApp'];
var fake = [];

for (var index = 0; index < 100000; index++) {
    fake.push({
        lat: +Math.random().toFixed(2)*100,
        lng: +Math.random().toFixed(2)*100,
        topics: [
            { keyword: keywords[Math.floor(Math.random()*keywords.length)], weight: 1 },
            { keyword: keywords[Math.floor(Math.random()*keywords.length)], weight: 1 },
            { keyword: keywords[Math.floor(Math.random()*keywords.length)], weight: 1 }
        ]
    });
}

fs.writeFileSync('../data/topics.fake.json', JSON.stringify(fake));