var placement = {}
var transitions = []
var previousTransitions = []
var previousPlacement = {}
var transitionMode = null
var transitionTime = null
var consumerData = {}

const isMemberKnown = (member) => Object.keys(placement).indexOf(member.host) !== -1

const addUpMember = (member) => {
    var memberName = member.host.replace('app', 'node')
    member.host = memberName
    if (!isMemberKnown(member)) {
        placement[memberName] = {
            operators: [],
            usage: 0
        }
        previousPlacement[memberName] = {
            operators: [],
            usage: 0
        }
        return
    }
    console.log('Ignoring already known member ' + memberName)
}

const setOperator = (mode, oldMember, oldOperator, member, operator, migTime) => {
    if (mode) {
        transitionMode = mode;
    }

    var memberName = member.host.replace('app', 'node')
    member.host = memberName
    if (!isMemberKnown(member)) {
        addUpMember(member)
        console.log(`Adding member that was previously unknown ${member.host}`)
    }
    var isTransition = false
    if (oldMember && oldOperator) {
        // TRANSITION
        // remove old operator from original host
        var oldMemberName = oldMember.host.replace('app', 'node')
        placement[oldMemberName].operators = placement[oldMemberName].operators.filter(op => {
            return oldOperator.name !== op.name
        });
        if (placement[oldMemberName].operators.length === 0) {
            placement[oldMemberName].usage = 2
        }
        transitions.push({source: oldMemberName, target: memberName, migrationTime: migTime, opName: operator.name})
        isTransition = true
    }

    let known = false
    let previousKnown = false
    for (let index in placement[memberName].operators) {
        const op = placement[memberName].operators[index]
        if (operator.name === op.name) {
            console.log('Placement already known, skipping')
            known = true
            break
        }
    }
    for (let index in previousPlacement[memberName].operators) {
        const op = previousPlacement[memberName].operators[index]
        if (operator.name === op.name) {
            console.log('Placement previously already known, skipping')
            previousKnown = true
            break
        }
    }
    if (!known) {
        placement[memberName].operators.push(operator)
        placement[memberName].usage = 1
    }
    if (!previousKnown && !isTransition) {
        previousPlacement[memberName].operators.push(operator)
    }
}
// called after all operators have transited
const setTransitionTime = (time) => {
    transitionTime = time
    previousTransitions = transitions
    transitions = []
}

const setConsumerData = (data) => {
    let coordinateData = {};
    Object.keys(data.coordinates).forEach((k) => {
        let nodeName = k.replace('app', 'node');
        coordinateData[nodeName] = data.coordinates[k];
    })
    data.latencyValues.forEach(v => {
        v.source = v.source.replace('app', 'node');
        v.destination = v.destination.replace('app', 'node');
    })
    data.coordinates = coordinateData;
    consumerData = data
}

const getConsumerData = (data) => {
    return consumerData
}

const getPlacement = () => placement
const getPreviousPlacement = () => previousPlacement
const getTransitions = () => transitions.concat(previousTransitions)
const getTransitionMode = () => transitionMode
const getTransitionTime = () => transitionTime
const clearTransitions = () => {
    transitions = []
    previousTransitions = []
    transitionTime = null

    // copy operators from the current placement
    Object.keys(previousPlacement).forEach(key => {
        previousPlacement[key].operators = placement[key].operators
    })
}
const clear = () => {placement = {}, transitions = [], previousPlacement = {}, transitionMode = null, transitionTime = null, consumerData = {}}

module.exports = {
    addUpMember,
    setOperator,
    setTransitionTime,
    setConsumerData,
    getConsumerData,
    getPlacement,
    getPreviousPlacement,
    getTransitions,
    getTransitionMode,
    getTransitionTime,
    clearTransitions,
    clear,
}