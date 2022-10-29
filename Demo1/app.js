import express from "express"
import routes from "./api/routes/index.js"

const PORT = process.env.PORT || 5001;

const app = express()

//Node js handles etags logic on its own
app.use(express.json())
app.set("etag", "strong")

routes(app)

app.listen(PORT, () => {
    console.log(`App listening on port ${PORT}`)
})