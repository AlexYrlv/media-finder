import React, { useState } from 'react';
import axios from 'axios';
import VideoList from './VideoList';

const backendUrl = process.env.REACT_APP_BACKEND_URL || 'http://localhost:8000';

const Search = () => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);

  const handleSearch = async (e) => {
    e.preventDefault();
    try {
      const response = await axios.post(`${backendUrl}/search/`, new URLSearchParams({ query }));
      setResults(response.data.results);
    } catch (error) {
      console.error("There was an error searching the videos!", error);
    }
  };

  return (
    <div>
      <h1>Search Videos</h1>
      <form onSubmit={handleSearch}>
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Enter search query"
        />
        <button type="submit">Search</button>
      </form>
      <VideoList videos={results} />
    </div>
  );
};

export default Search;
