import React, { useState } from 'react';
import axios from 'axios';

const backendUrl = process.env.REACT_APP_BACKEND_URL || 'http://localhost:8000';

const Upload = () => {
  const [link, setLink] = useState('');
  const [response, setResponse] = useState(null);

  const handleUpload = async (e) => {
    e.preventDefault();
    try {
      const response = await axios.post(`${backendUrl}/upload_video_by_link/`, { link });
      setResponse(response.data);
    } catch (error) {
      console.error("There was an error uploading the video!", error);
    }
  };

  return (
    <div>
      <h1>Upload Video by Link</h1>
      <form onSubmit={handleUpload}>
        <input
          type="text"
          value={link}
          onChange={(e) => setLink(e.target.value)}
          placeholder="Enter video link"
        />
        <button type="submit">Upload</button>
      </form>
      {response && (
        <div>
          <h2>Response</h2>
          <pre>{JSON.stringify(response, null, 2)}</pre>
        </div>
      )}
    </div>
  );
};

export default Upload;
