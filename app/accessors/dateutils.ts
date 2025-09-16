export function generateDailyDates(from: Date, to: Date): Date[] {
    const result: Date[] = []

    // Normalize start date to 09:00
    let current = new Date(from)
    current.setHours(9, 0, 0, 0)

    // Also normalize end date to 09:00
    const end = new Date(to)
    end.setHours(9, 0, 0, 0)

    while (current <= end) {
        result.push(new Date(current)) // clone to avoid reference issues
        current.setDate(current.getDate() + 1) // move to next day
    }

    return result
}

export function getDayOfYear(date: Date): number {
    const startOfYear = new Date(date.getFullYear(), 0, 1) // Jan 1, midnight
    const diff = date.getTime() - startOfYear.getTime()
    const oneDayMs = 1000 * 60 * 60 * 24
    return Math.floor(diff / oneDayMs) + 1 // +1 because Jan 1 is day 1
}

import axios from "axios"
import { createWriteStream } from "fs"
import { basename } from "path"

export async function downloadFile(url: string, outputPath?: string): Promise<void> {
    const filename = outputPath ?? basename(url)

    console.debug(`Downloading ${url} -> ${filename}`)

    const response = await axios.get(url, { responseType: "stream" })
    const writer = createWriteStream(filename)

    return new Promise((resolve, reject) => {
        response.data.pipe(writer)
        writer.on("finish", resolve)
        writer.on("error", reject)
    })
}
