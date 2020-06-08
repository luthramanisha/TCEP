const express = require('express')
const app = express()
const bodyParser = require('body-parser')
const morgan = require('morgan')
const morganBody = require('morgan-body')
const winston = require('winston')
const cors = require('cors')

const route = require('./route')

app.use(cors({
    origin: '*',
    optionsSuccessStatus: 200
}))
app.use(bodyParser.json())

morganBody(app)

app.use(morgan('tiny'))
app.use(route)

app.listen(3000, () => {
    console.log('Started TCEP data server')
})