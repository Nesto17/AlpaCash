import { useState } from 'react';
import { Form, Button, Alert } from 'react-bootstrap';
import axios from 'axios';
import './App.css';

const BASE_URL = 'http://127.0.0.1:5000/';

function App() {
    const [ticker, setTicker] = useState('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [lastInputtedTicker, setLastInputtedTicker] = useState('');
    const [plotHtml, setPlotHtml] = useState('');

    const predictForecast = () => {
        setLoading(true);
        setError('');

        axios
            .get(`${BASE_URL}forecast/${ticker}`)
            .then((res) => {
                setLastInputtedTicker(ticker);
                setPlotHtml(res.data.html);
            })
            .catch((err) => {
                setError(err.response.data.error_message);
            })
            .finally(() => {
                setLoading(false);
            });
    };

    return (
        <>
            <div className="pt-5 content">
                <div className="content-text">
                    <h1 className="display-1 fw-bold content-text_title">🦙 Alpacash</h1>
                    <h3 className="content-text_tagline">Daily stocks forecasting</h3>
                </div>
                <Form className="mt-5 content-input">
                    <Form.Label className="content-input_label">Enter ticker below:</Form.Label>
                    <div className="content-input_interact">
                        <Form.Control
                            className="content-input_field"
                            type="text"
                            value={ticker}
                            onChange={(e) => setTicker(e.target.value)}
                        />
                        <Button
                            className="ms-3 content-input_submit"
                            onClick={predictForecast}
                            disabled={loading}
                        >
                            Forecast
                        </Button>
                    </div>
                    {error !== '' && (
                        <Alert className="mt-3" variant="danger">
                            Ticker not found! <br />
                            See all available tickers{' '}
                            <Alert.Link href="https://github.com/Nesto17/AlpaCash/blob/main/tickers/all_tickers.csv">
                                here
                            </Alert.Link>
                        </Alert>
                    )}
                </Form>
                <div className="mt-5 content-extra">
                    <p className="content-extra-desc">Data is updated daily at 11.15 AM UTC+0</p>
                    <p className="mt-1 content-extra-github">
                        GitHub, roadmap, and more details can be found{' '}
                        <a
                            href="https://github.com/Nesto17/AlpaCash"
                            rel="noopener noreferrer"
                            target="_blank"
                        >
                            here
                        </a>
                    </p>
                </div>
            </div>
            <div className="plot-container">
                {lastInputtedTicker !== '' && (
                    <h1 className="fw-bold fst-italic plot-title">
                        {lastInputtedTicker.toUpperCase()} 1-Year Forecast
                    </h1>
                )}
                <iframe title="a" className="pt-2 plot" srcDoc={plotHtml}></iframe>
            </div>
        </>
    );
}

export default App;
