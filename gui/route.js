const express = require('express')
const request = require('request')
const router = express.Router()
const Placement = require('./placement')
const Constants = require('./constants')
// communication with TCEPSocket of simulator application
console.log(Constants.SERVER)

router.get('/data', (req, res) => {
    const placement = Placement.getPlacement()
    const previousPlacement = Placement.getPreviousPlacement()
    const transitions = Placement.getTransitions()
    const transitionMode = Placement.getTransitionMode()
    const transitionTime = Placement.getTransitionTime()
    const consumerData = Placement.getConsumerData()
    const nodes = []
    const previousNodes = []
    for (let key in placement) {
        nodes.push({
            name: key,
            operators: placement[key].operators,
            usage: placement[key].usage
        })
    }
    for (let key in previousPlacement) {
        previousNodes.push({
            name: key,
            operators: previousPlacement[key].operators,
            usage: previousPlacement[key].usage
        })
    }
    request(`http://${Constants.SERVER}:${Constants.TCEP_PORT}/status`, (err, resp, body) => {
        const response = { nodes, previousNodes, transitions, transitionMode, transitionTime, consumerData }
        if (!err) {
            response['status'] = JSON.parse(body);
        }
        res.send(response);
    })
});

router.post('/setOperator', (req, res) => {
    Placement.setOperator(req.body.transitionMode, req.body.oldMember, req.body.oldOperator, req.body.member, req.body.operator, req.body.migrationTime)
    res.send({})
});

router.post('/setMembers', (req, res) => {
    req.body.members.forEach((member) => Placement.addUpMember(member))
    res.send({})
});

router.post('/setTransitionTime', (req, res) => {
    Placement.setTransitionTime(req.body.time)
    res.send({})
});

router.post('/consumer', (req, res) => {
    Placement.setConsumerData(req.body);
    res.send({})
})

router.delete('/data', (req, res) => {
    Placement.clear()
    res.send({})
})

router.get('/', (req, res) => {
    res.sendFile('graph.html', { root : __dirname})
})

router.use('/src', express.static('src'))

router.get('/algorithms', (req, res) => {
    request(`http://${Constants.SERVER}:${Constants.TCEP_PORT}/algorithms`, (err, resp, body) => {
        res.send(body);
    })
})

router.get('/status', (req, res) => {
    request(`http://${Constants.SERVER}:${Constants.TCEP_PORT}/status`, (err, resp, body) => {
        res.send(body);
    })
})

router.post('/transition', (req, res) => {
    Placement.clearTransitions()
    request.post(`http://${Constants.SERVER}:${Constants.TCEP_PORT}/transition`, { form: JSON.stringify(req.body) }, (err, resp, body) => {
        res.send(body);
    })
})

router.post('/manualTransition', (req, res) => {
    Placement.clearTransitions()
    request.post(`http://${Constants.SERVER}:${Constants.TCEP_PORT}/manualTransition`, { form: JSON.stringify(req.body) }, (err, resp, body) => {
        res.send(body);
    })
})

router.post('/start', (req, res) => {
    request.post(`http://${Constants.SERVER}:${Constants.TCEP_PORT}/start`, { form: JSON.stringify(req.body) }, (err, resp, body) => {
        res.send(body);
    })
})

router.post('/stop', (req, res) => {
    Placement.clear()
    request.post(`http://${Constants.SERVER}:${Constants.TCEP_PORT}/stop`, (err, resp, body) => {
        res.send(body);
    })
})

router.post('/autoTransition', (req, res) => {
    request.post(`http://${Constants.SERVER}:${Constants.TCEP_PORT}/autoTransition`, { form: JSON.stringify(req.body) }, (err, resp, body) => {
        res.send(body);
    })
})

router.use('/resources', express.static(__dirname + '/resources'));

module.exports = router