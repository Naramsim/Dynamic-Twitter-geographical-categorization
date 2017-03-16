var fs = require('fs')
var keywords = ['Matteo Renzi', 'Atalanta Bergamasca Calcio', 'Nintendo Switch', 'Luciano Spalletti', 'Matteo Salvini', 'Associazione Sportiva Roma', 'Paesi Bassi', 'Palermo', 'Mark Zuckerberg', 'WhatsApp']
var fake = []

for (var index = 0; index < 10000; index++) {
    fake.push({
        lat: +Math.random().toFixed(2),
        lng: +Math.random().toFixed(2),
        topics: [
            { keyword: keywords[Math.floor(Math.random()*keywords.length)], weight: +Math.random().toFixed(2) },
            { keyword: keywords[Math.floor(Math.random()*keywords.length)], weight: +Math.random().toFixed(2) },
            { keyword: keywords[Math.floor(Math.random()*keywords.length)], weight: +Math.random().toFixed(2) }
        ]
    })
}

fs.writeFileSync('./share/URLCat/topics.fake.json', JSON.stringify(fake));