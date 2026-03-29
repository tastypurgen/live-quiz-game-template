import 'dotenv/config';

import { randomUUID } from 'node:crypto';
import { WebSocket, WebSocketServer } from 'ws';

import type {
  CreateGameData,
  Game,
  JoinGameData,
  Player,
  Question,
  RegData,
  StartGameData,
  User,
  WSMessage,
} from './types.js';

const PORT = process.env.PORT ? Number.parseInt(process.env.PORT, 10) : 3000;
const ROOM_CODE_LENGTH = 6;
const ROOM_CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const usersByName = new Map<string, User>();
const usersById = new Map<string, User>();
const gamesById = new Map<string, Game>();
const gamesByCode = new Map<string, Game>();
const socketUsers = new Map<WebSocket, string>();

const wss = new WebSocketServer({ port: PORT });

const sendMessage = (ws: WebSocket, type: string, data: unknown) => {
  if (ws.readyState !== WebSocket.OPEN) {
    return;
  }

  ws.send(
    JSON.stringify({
      type,
      data,
      id: 0,
    }),
  );
};

const sendError = (ws: WebSocket, message: string) => {
  sendMessage(ws, 'error', { message });
};

const sanitizePlayer = (player: Player) => ({
  name: player.name,
  index: player.index,
  score: player.score,
});

const getGameSockets = (game: Game) => {
  const sockets = new Set<WebSocket>();
  const host = usersById.get(game.hostId);

  if (host?.ws?.readyState === WebSocket.OPEN) {
    sockets.add(host.ws);
  }

  for (const player of game.players) {
    if (player.ws?.readyState === WebSocket.OPEN) {
      sockets.add(player.ws);
    }
  }

  return [...sockets];
};

const broadcastToGame = (game: Game, type: string, data: unknown) => {
  for (const ws of getGameSockets(game)) {
    sendMessage(ws, type, data);
  }
};

const broadcastPlayers = (game: Game) => {
  broadcastToGame(
    game,
    'update_players',
    game.players.map(sanitizePlayer),
  );
};

const generateRoomCode = () => {
  let code = '';

  do {
    code = Array.from({ length: ROOM_CODE_LENGTH }, () => {
      const index = Math.floor(Math.random() * ROOM_CODE_ALPHABET.length);
      return ROOM_CODE_ALPHABET[index];
    }).join('');
  } while (gamesByCode.has(code));

  return code;
};

const getRegisteredUser = (ws: WebSocket) => {
  const userId = socketUsers.get(ws);

  if (!userId) {
    return undefined;
  }

  return usersById.get(userId);
};

const findGameByHost = (userId: string) => {
  for (const game of gamesById.values()) {
    if (game.hostId === userId) {
      return game;
    }
  }

  return undefined;
};

const findGameByPlayer = (userId: string) => {
  for (const game of gamesById.values()) {
    if (game.players.some((player) => player.index === userId)) {
      return game;
    }
  }

  return undefined;
};

const findGameForUser = (userId: string) => findGameByHost(userId) ?? findGameByPlayer(userId);

const validateQuestion = (question: Question) => {
  if (typeof question.text !== 'string' || question.text.trim().length === 0) {
    return false;
  }

  if (!Array.isArray(question.options) || question.options.length !== 4) {
    return false;
  }

  if (question.options.some((option) => typeof option !== 'string' || option.trim().length === 0)) {
    return false;
  }

  if (!Number.isInteger(question.correctIndex) || question.correctIndex < 0 || question.correctIndex > 3) {
    return false;
  }

  if (!Number.isFinite(question.timeLimitSec) || question.timeLimitSec <= 0) {
    return false;
  }

  return true;
};

const sendQuestion = (game: Game, questionIndex: number) => {
  const question = game.questions[questionIndex];

  game.currentQuestion = questionIndex;
  game.questionStartTime = Date.now();
  game.playerAnswers.clear();

  broadcastToGame(game, 'question', {
    questionNumber: questionIndex + 1,
    totalQuestions: game.questions.length,
    text: question.text,
    options: question.options,
    timeLimitSec: question.timeLimitSec,
  });
};

const handleRegistration = (ws: WebSocket, data: unknown) => {
  const payload = data as RegData;

  if (
    !payload ||
    typeof payload.name !== 'string' ||
    typeof payload.password !== 'string' ||
    payload.name.trim().length === 0 ||
    payload.password.trim().length === 0
  ) {
    sendMessage(ws, 'reg', {
      name: '',
      index: '',
      error: true,
      errorText: 'Name and password are required.',
    });
    return;
  }

  const name = payload.name.trim();
  const password = payload.password.trim();
  let user = usersByName.get(name);

  if (user && user.password !== password) {
    sendMessage(ws, 'reg', {
      name,
      index: '',
      error: true,
      errorText: 'Invalid password.',
    });
    return;
  }

  if (!user) {
    user = {
      name,
      password,
      index: randomUUID(),
      ws,
    };

    usersByName.set(name, user);
    usersById.set(user.index, user);
  } else if (user.ws && user.ws !== ws) {
    user.ws.close(1000, 'Session replaced by a new connection.');
  }

  user.ws = ws;
  socketUsers.set(ws, user.index);

  sendMessage(ws, 'reg', {
    name: user.name,
    index: user.index,
    error: false,
    errorText: '',
  });
};

const handleCreateGame = (ws: WebSocket, data: unknown) => {
  const user = getRegisteredUser(ws);

  if (!user) {
    sendError(ws, 'You must register before creating a game.');
    return;
  }

  const existingGame = findGameForUser(user.index);

  if (existingGame && existingGame.status !== 'finished') {
    sendError(ws, 'You are already participating in another game.');
    return;
  }

  const payload = data as CreateGameData;

  if (!payload || !Array.isArray(payload.questions) || payload.questions.length === 0) {
    sendError(ws, 'At least one valid question is required.');
    return;
  }

  if (!payload.questions.every(validateQuestion)) {
    sendError(ws, 'Each question must have 4 options, a valid correct answer, and a positive time limit.');
    return;
  }

  const game: Game = {
    id: randomUUID(),
    code: generateRoomCode(),
    hostId: user.index,
    questions: payload.questions,
    players: [],
    currentQuestion: -1,
    status: 'waiting',
    playerAnswers: new Map(),
  };

  gamesById.set(game.id, game);
  gamesByCode.set(game.code, game);

  sendMessage(ws, 'game_created', {
    gameId: game.id,
    code: game.code,
  });
};

const handleJoinGame = (ws: WebSocket, data: unknown) => {
  const user = getRegisteredUser(ws);

  if (!user) {
    sendError(ws, 'You must register before joining a game.');
    return;
  }

  const existingGame = findGameForUser(user.index);

  if (existingGame && existingGame.status !== 'finished') {
    sendError(ws, 'You are already participating in another game.');
    return;
  }

  const payload = data as JoinGameData;
  const code = payload?.code?.trim().toUpperCase();

  if (!code) {
    sendError(ws, 'Room code is required.');
    return;
  }

  const game = gamesByCode.get(code);

  if (!game) {
    sendError(ws, 'Game not found.');
    return;
  }

  if (game.status !== 'waiting') {
    sendError(ws, 'This game has already started.');
    return;
  }

  if (game.hostId === user.index) {
    sendError(ws, 'Host cannot join their own game as a player.');
    return;
  }

  const player: Player = {
    name: user.name,
    index: user.index,
    score: 0,
    ws,
  };

  game.players.push(player);

  sendMessage(ws, 'game_joined', {
    gameId: game.id,
  });

  broadcastToGame(game, 'player_joined', {
    playerName: player.name,
    playerCount: game.players.length,
  });
  broadcastPlayers(game);
};

const handleStartGame = (ws: WebSocket, data: unknown) => {
  const user = getRegisteredUser(ws);

  if (!user) {
    sendError(ws, 'You must register before starting a game.');
    return;
  }

  const payload = data as StartGameData;
  const game = payload?.gameId ? gamesById.get(payload.gameId) : undefined;

  if (!game) {
    sendError(ws, 'Game not found.');
    return;
  }

  if (game.hostId !== user.index) {
    sendError(ws, 'Only the host can start the game.');
    return;
  }

  if (game.status !== 'waiting') {
    sendError(ws, 'Game has already started.');
    return;
  }

  if (game.players.length === 0) {
    sendError(ws, 'At least one player must join before starting.');
    return;
  }

  game.status = 'in_progress';
  sendQuestion(game, 0);
};

const handleMessage = (ws: WebSocket, rawMessage: Buffer) => {
  let message: WSMessage;

  try {
    message = JSON.parse(rawMessage.toString()) as WSMessage;
  } catch {
    sendError(ws, 'Invalid JSON payload.');
    return;
  }

  if (!message || typeof message.type !== 'string') {
    sendError(ws, 'Invalid message format.');
    return;
  }

  switch (message.type) {
    case 'reg':
      handleRegistration(ws, message.data);
      return;
    case 'create_game':
      handleCreateGame(ws, message.data);
      return;
    case 'join_game':
      handleJoinGame(ws, message.data);
      return;
    case 'start_game':
      handleStartGame(ws, message.data);
      return;
    case 'answer':
      sendError(ws, 'Answer handling is not implemented yet.');
      return;
    default:
      sendError(ws, `Unsupported message type: ${message.type}`);
  }
};

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    handleMessage(ws, Buffer.isBuffer(message) ? message : Buffer.from(message.toString()));
  });
});

console.log(`WebSocket server running at ws://localhost:${PORT}`);
