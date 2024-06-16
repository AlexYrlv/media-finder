import React from 'react';

const VideoList = ({ videos }) => {
  return (
    <div>
      <h2>Video Results</h2>
      <ul>
        {videos.map((video, index) => (
          <li key={index}>
            <a href={video} target="_blank" rel="noopener noreferrer">
              {video}
            </a>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default VideoList;
