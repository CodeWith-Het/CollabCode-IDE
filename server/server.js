require("dotenv").config({ path: __dirname + "/.env" });
const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");
const mongoose = require("mongoose");
const app = require("./src/app"); 
const path = require("path")
const { applyOp, transform } = require("./src/ot/textOt");

const server = http.createServer(app);
app.use(cors());

const io = new Server(server, {
  cors: {
    origin: "*", // Production ke liye sahi se set kar lena
    methods: ["GET", "POST"],
  },
});

const userSocketMap = {};
const roomFileTrees = new Map();
const roomDocs = new Map();

function getOrCreateFileDoc(roomId, fileId) {
  const key = `${roomId}_${fileId}`;
  if (!roomDocs.has(key)) {
    roomDocs.set(key, {
      text: "",
      version: 0,
      history: [],
      historyStartVersion: 1,
      language: "javascript",
    });
  }
  return roomDocs.get(key);
}

function getAllConnectedClients(roomId) {
  const roomClients = Array.from(io.sockets.adapter.rooms.get(roomId) || []);
  return roomClients.map((socketId) => ({
    socketId,
    username: userSocketMap[socketId],
  }));
}

// MongoDB Connection with proper error handling
mongoose
  .connect(process.env.MONGO_URL)
  .then(() => console.log("MongoDB Connected 🟢"))
  .catch((err) => console.error("MongoDB Connection Error 🔴:", err));

  app.use(express.static(path.join(__dirname, "public")));

  app.get("/*", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "index.html"));
  });

io.on("connection", (socket) => {
  console.log(`Socket Connected: ${socket.id}`);

  socket.on("join", ({ roomId, username }) => {
    if (!username) return;

    // Smart Disconnect Old Instance
    const clients = getAllConnectedClients(roomId);
    const existingClient = clients.find((c) => c.username === username);
    if (existingClient) {
      const oldSocket = io.sockets.sockets.get(existingClient.socketId);
      if (oldSocket) {
        oldSocket.leave(roomId);
        delete userSocketMap[oldSocket.id];
        socket
          .in(roomId)
          .emit("disconnected", { socketId: oldSocket.id, username });
      }
    }

    userSocketMap[socket.id] = username;
    socket.join(roomId);

    if (roomFileTrees.has(roomId)) {
      socket.emit("file_structure_update", roomFileTrees.get(roomId));
    }

    io.in(roomId).emit("joined", {
      clients: getAllConnectedClients(roomId),
      username,
      socketId: socket.id,
    });
  });

  socket.on("file_structure_change", ({ roomId, files }) => {
    if (!roomId) return;
    roomFileTrees.set(roomId, files);
    socket.to(roomId).emit("file_structure_update", files);
  });

  socket.on("request_file_sync", ({ roomId, fileId }) => {
    if (!roomId || !fileId) return;
    const doc = getOrCreateFileDoc(roomId, fileId);
    socket.emit("doc_init", {
      fileId,
      text: doc.text,
      version: doc.version,
      language: doc.language,
    });
  });

  socket.on("ot_op", ({ roomId, fileId, op, baseVersion }) => {
    if (!roomId || !fileId || !op) return;

    const doc = getOrCreateFileDoc(roomId, fileId);
    let incoming = op;
    const base = Number.isFinite(baseVersion) ? baseVersion : doc.version;

    // History transformation
    doc.history.forEach((hist) => {
      if (hist.version > base) incoming = transform(incoming, hist.op);
    });

    doc.text = applyOp(doc.text, incoming);
    doc.version += 1;
    doc.history.push({ version: doc.version, op: incoming });

    // History cleanup
    if (doc.history.length > 200) {
      doc.history.splice(0, doc.history.length - 200);
      doc.historyStartVersion = doc.history[0].version;
    }

    io.to(roomId).emit("ot_applied", {
      fileId,
      op: incoming,
      version: doc.version,
      authorSocketId: socket.id,
    });
  });

  socket.on("language_change", ({ roomId, fileId, language }) => {
    const doc = getOrCreateFileDoc(roomId, fileId);
    doc.language = language;
    io.to(roomId).emit("language_changed", { language, fileId });
  });

  socket.on("disconnecting", () => {
    [...socket.rooms].forEach((roomId) => {
      if (roomId !== socket.id) {
        socket.in(roomId).emit("disconnected", {
          socketId: socket.id,
          username: userSocketMap[socket.id],
        });

        // Auto-cleanup memory
        const room = io.sockets.adapter.rooms.get(roomId);
        if (!room || room.size <= 1) {
          roomFileTrees.delete(roomId);
          for (let key of roomDocs.keys()) {
            if (key.startsWith(`${roomId}_`)) roomDocs.delete(key);
          }
        }
      }
    });
    delete userSocketMap[socket.id];
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`SERVER RUNNING ON PORT ${PORT} 🚀`);
});
