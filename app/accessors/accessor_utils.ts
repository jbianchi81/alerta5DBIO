import axios, { AxiosRequestConfig, AxiosError } from "axios";
import https from "https";

export interface FetchDataOptions extends AxiosRequestConfig {
    disable_validation?: boolean;
}

export async function fetchData<T = unknown>(
    url: string,
    options?: FetchDataOptions
): Promise<T> {
    const agent = new https.Agent({
        rejectUnauthorized: !options?.disable_validation,
    });

    try {
        const response = await axios.get<T>(url, {
            ...options,
            httpsAgent: agent,
        });

        return response.data;
    } catch (err) {
        const error = err as AxiosError<{ message?: string }>;

        if (error.response) {
            const status = error.response.status;
            const message =
                error.response.data?.message ?? error.message;

            throw new Error(
                `Request failed with status ${status}: ${message}`
            );
        }

        throw new Error(error.message || "Unknown error");
    }
}

export function parseUtcDateTime(s: string): Date {
    const [date, time] = s.split(" ");
    const [year, month, day] = date.split("-").map(Number);
    const [hour, minute, second] = time.split(":").map(Number);

    return new Date(Date.UTC(year, month - 1, day, hour, minute, second));
}