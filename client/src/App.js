import { useState } from 'react';
import { Form, Button, Alert } from 'react-bootstrap';
import axios from 'axios';
import './App.css';

const BASE_URL = 'http://127.0.0.1:5000/';

function App() {
    const [ticker, setTicker] = useState('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [plotHtml, setPlotHtml] = useState('');

    const predictForecast = () => {
        setLoading(true);
        setError('');

        axios
            .get(`${BASE_URL}forecast/${ticker}`)
            .then((res) => {
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
            <div className="content">
                <div className="content-text">
                    <h1 className="display-1 fw-bold content-text_title">🦙 Alpacash</h1>
                    <h3 className="content-text_tagline">Daily stocks forecasting</h3>
                </div>
                <Form className="content-input">
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
                            <Alert.Link href="https://google.com">here</Alert.Link>
                        </Alert>
                    )}
                </Form>
            </div>
            <div className="plot-container">
                <iframe title="a" className="plot" srcDoc={plotHtml}></iframe>
            </div>
        </>
    );
}

export default App;
