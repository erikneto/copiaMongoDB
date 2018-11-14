const request = require('requestretry');
const MongoClient = require('mongodb').MongoClient;
//const sessoes = require('./protocolos').map(i => i._id);
const dia = '13'

const urlChat = ``
const urlSessionTotal = ``
const startDate = "2018-11-07T00:00:00.000Z"
const endDate = "2018-11-10T00:00:00.000Z"
let concorrentes = 0
let tempoInicio = 0
let executado = 0
let total = 0


main();

async function main() {
    const mongoDB = await MongoClient.connect("mongodb://sntd:abc123@localhost:27017/admin", {
        useNewUrlParser: true
    });
    console.log("Connected successfully to server");
    console.log("Buscando sessões");
    let sessoes = await lerSessoes()

    console.log(`Importando ${sessoes.length} sessões`);
    tempoInicio = new Date();
    total = sessoes.length;
    prmTodasSessoes = sessoes.map(s => adicionarSessaoCompleta(mongoDB, s));
    Promise.all(prmTodasSessoes).then((result) => {
        console.log('Importação concluída com sucesso')
        mongoDB.close();
    })

}

function adicionarSessaoCompleta(mongoDB, sessao) {
    return new Promise(async (resolve, reject) => {
        // let done = false;
        // setTimeout(() => {
        //     if (!done) {
        //         if (concurrent > 0) {
        //             concurrent--;
        //         }
        //     }
        // }, 36000);
        while (concorrentes > 500) {
            await timeout(1000);
        }
        concorrentes++;
        if (!(await jaImportado(mongoDB, sessao.protocol))) {
            let chats = await lerChat(sessao._id);
            if (chats.length) {
                const idSessao = await adicionarNovaSessao(mongoDB, sessao, chats[0].createdAt)
                chats = chats.map(c => {
                    c.session = idSessao;
                    c.createdAt = new Date(c.createdAt);
                    c.updatedAt = new Date(c.updatedAt);
                    delete c._id;
                    return c;
                });
                await adicionarChats(mongoDB, chats);
            }
        }
        const tempoCorrido = (new Date()) - tempoInicio;
        executado++;
        const percConcluido = executado / total
        //total = tempoCorrido / parcial - tempoCorrido

        showInfo(`\x1b[34mProtocolo ${sessao.protocol} => ${(percConcluido*100).toFixed(2)}%. \x1b[32mTempo estimado para terminar: ${((tempoCorrido / percConcluido - tempoCorrido)/1000).toFixed(0)} segundos`)
        concorrentes--;
        return resolve();
    });
}

function lerSessoes() {
    return new Promise(function (resolve, reject) {
        request({
            url: urlSessionTotal,
            method: 'POST',
            json: {
                where: {
                    $and: [{
                            createdAt: {
                                $gte: new Date(startDate)
                            }
                        },
                        {
                            createdAt: {
                                $lte: new Date(endDate)
                            }
                        },
                    ]
                }
            },

            maxAttempts: 1000,
            retryDelay: 5000,
            strictSSL: false
        }, (err, response, body) => {
            if (err) {
                console.error(err);
                return reject(err);
            }
            return resolve(body.objReturn.map(s => {
                s.createdAt = new Date(s.createdAt)
                s.updatedAt = new Date(s.updatedAt)
                return s;
            }))
        })
    });
}

function adicionarChats(db, chats) {
    return new Promise(function (resolve, reject) {
        db.db('admin').collection('chats').insertMany(chats, (err, result) => {
            return resolve(true);
        });
    });
}

function adicionarNovaSessao(db, sessao, createdAt) {
    return new Promise(function (resolve, reject) {
        db.db('admin').collection('sessions').insertOne(sessao, (err, response) => {

            return resolve(response.ops[0]._id);
        })

    });
}


function jaImportado(db, protocol) {
    return new Promise(function (resolve, reject) {
        db.db('admin').collection('sessions').find({
            protocol: protocol
        }).toArray((err, docs) => {
            return resolve(docs.length);
        })

    });

}

function lerChat(idSessao) {
    return new Promise(function (resolve, reject) {
        request({
            url: urlChat,
            method: 'POST',
            json: {
                where: {
                    session: idSessao
                }
            },
            maxAttempts: 1000,
            retryDelay: 5000,
            strictSSL: false
        }, (err, response, body) => {
            if (err) {
                console.error(err);
                return reject(err);
            }
            return resolve(body.objReturn)
        })
    });
}

function showInfo(info) {
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(info);

}

function timeout(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
};